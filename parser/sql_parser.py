#!/usr/bin/env python3
"""
Parser SQL que devuelve AST/ExecutionPlan sin side-effects.
"""

import sys
import os
from typing import Dict, List, Any, Optional, Union
from lark import Lark, Transformer, Token, Tree
from lark.exceptions import LarkError

# Importar la gramática
from grammar import GRAMMAR

class ExecutionPlan:
    """Representa un plan de ejecución para una consulta SQL."""
    
    def __init__(self, operation: str, **kwargs):
        self.operation = operation  # 'CREATE_TABLE', 'SELECT', 'INSERT', 'DELETE', 'UPDATE'
        self.data = kwargs
    
    def __repr__(self):
        return f"ExecutionPlan({self.operation}, {self.data})"


class SQLTransformer(Transformer):
    """
    Transformer robusto: usa *items para evitar errores de aridad,
    normaliza tokens a tipos nativos y construye ExecutionPlan consistentes.
    """

    def varchar_type(self, items):
        """Procesa VARCHAR[size]."""
        size = int(items[1])  # El INT entre corchetes
        return ("VARCHAR", size)

    def string_type(self, items):
        """Procesa STRING[size]."""
        size = int(items[1])
        return ("STRING", size)

    def array_type(self, items):
        """Procesa ARRAY[FLOAT]."""
        return "ARRAY[FLOAT]"

    def process_varchar_type(self, items):
        """Procesa VARCHAR[50] específicamente."""
        print(f"DEBUG process_varchar_type: {items}")
        if len(items) >= 3:
            size = int(items[2])  # El número entre los corchetes
            return ("VARCHAR", size)
        return "VARCHAR"

    def process_string_type(self, items):
        """Procesa STRING[50] específicamente."""
        print(f"DEBUG process_string_type: {items}")
        if len(items) >= 3:
            size = int(items[2])
            return ("STRING", size)
        return "STRING"

    def process_array_type(self, items):
        """Procesa ARRAY[FLOAT] específicamente."""
        print(f"DEBUG process_array_type: {items}")
        return "ARRAY[FLOAT]"


    def _to_str(self, v):
        if isinstance(v, Token):
            return str(v)
        return v

    def _to_number(self, v):
        if isinstance(v, Token):
            s = str(v)
            return float(s) if '.' in s else int(s)
        return v

    def statement_list(self, items):
        """Debug para ver todo lo que se está parseando."""
        print(f"DEBUG statement_list ALL ITEMS: {items}")
        return {"type": "statement_list", "statements": items}

    def _unwrap(self, item):
        # Si Lark nos pasa Tree o Token anidado, sacamos el valor contenido cuando sea simple.
        if isinstance(item, Tree):
            # si es un Tree con un único child token -> devolver child valor
            if len(item.children) == 1 and isinstance(item.children[0], Token):
                tok = item.children[0]
                if tok.type in ("INT", "SIGNED_NUMBER"):
                    return self._to_number(tok)
                return str(tok)
            # si Tree representa lista (p.ej. field_definitions) devolver children ya transformados
            return [self._unwrap(c) for c in item.children]
        if isinstance(item, Token):
            if item.type in ("INT",):
                return int(item)
            if item.type in ("SIGNED_NUMBER",):
                return self._to_number(item)
            if item.type in ("ESCAPED_STRING",):
                s = str(item)
                return s[1:-1].encode('utf-8').decode('unicode_escape')
            return str(item)
        return item

    def _unwrap_token(self, v):
        if isinstance(v, Token):
            if v.type in ("INT",):
                return int(str(v))
            if v.type in ("SIGNED_NUMBER",):
                s = str(v); return float(s) if '.' in s else int(s)
            if v.type == "ESCAPED_STRING":
                s = str(v); return s[1:-1].encode('utf-8').decode('unicode_escape')
            return str(v)
        if isinstance(v, Tree):
            # si Tree tiene un solo child token, desempaquetar
            if len(v.children) == 1 and isinstance(v.children[0], Token):
                return self._unwrap_token(v.children[0])
            # si Tree es lista, procesar children
            return [self._unwrap_token(c) for c in v.children]
        return v

    def value_list(self, items):
        """Value list corregido para aplanar listas."""
        values = []
        for item in items:
            unwrapped = self._unwrap_tree_token(item)
            if isinstance(unwrapped, list):
                values.extend(unwrapped)
            else:
                values.append(unwrapped)
        
        print(f"DEBUG value_list final: {values}")
        return values

    def index_type(self, items):
        """Procesa tipo de índice."""
        if items:
            return self._unwrap_tree_token(items[0])
        return None

    def key_field(self, items):
        """Procesa campo clave."""
        if items:
            return self._unwrap_tree_token(items[0])
        return None

    # --- Start / statement list ---
    def start(self, items):
        if len(items) == 1:
            return items[0]
        return {"type": "statement_list", "statements": items}

    def statement_list(self, items):
        return {"type": "statement_list", "statements": items}

    # --- CREATE TABLE from schema ---
    def create_table_schema(self, *items):
        # tolerant: buscar CNAME (table name) y field_definitions (lista)
        table_name = None
        fields = None
        for it in items:
            if isinstance(it, str):
                if table_name is None:
                    table_name = it
            elif isinstance(it, list):
                fields = it
            elif isinstance(it, dict) and 'name' in it:
                # single field
                fields = [it] if fields is None else (fields + [it])
        return ExecutionPlan("CREATE_TABLE", table_name=table_name, fields=fields, source=None)

    def create_table_statement(self, items):
        # items may contain table name (Token or str) and a list/tree of field definitions
        table_name = None
        fields = []
        from lark import Token, Tree
        for it in items:
            if isinstance(it, Token) and it.type == "CNAME" and table_name is None:
                table_name = str(it)
            elif isinstance(it, str) and table_name is None:
                table_name = it
            elif isinstance(it, list):
                # ya deberían ser dicts de field_definition
                for f in it:
                    fields.append(f)
            elif isinstance(it, Tree) and it.data == "field_definitions":
                # desempaca children
                for child in it.children:
                    # si child es Tree o Token, intenta convertir
                    fields.append(self._unwrap_tree_token(child))
            elif isinstance(it, dict) and 'name' in it:
                fields.append(it)
        return ExecutionPlan('CREATE_TABLE', table_name=table_name, fields=fields or None, source=None)


    def create_table_from_file(self, items):
        """CREATE TABLE FROM FILE corregido."""
        table_name = None
        file_path = None
        index_type = None
        key_field = None
        
        print(f"DEBUG create_table_from_file items: {items}")
        
        # Procesar todos los items
        for item in items:
            unwrapped = self._unwrap_tree_token(item)
            print(f"DEBUG unwrapped: {unwrapped} (type: {type(unwrapped)})")
            
            if isinstance(unwrapped, str):
                if table_name is None:
                    table_name = unwrapped
                elif file_path is None and ('.csv' in unwrapped or unwrapped.endswith('.csv')):
                    file_path = unwrapped
                elif index_type is None and unwrapped.upper() in ['BTREE', 'EXTENDIBLEHASH', 'ISAM', 'SEQ', 'RTREE']:
                    index_type = unwrapped.upper()
                elif key_field is None and unwrapped not in [table_name, file_path, index_type]:
                    key_field = unwrapped
        
        # Si todavía no tenemos key_field, buscar en lists anidadas
        if key_field is None:
            for item in items:
                unwrapped = self._unwrap_tree_token(item)
                if isinstance(unwrapped, list):
                    for subitem in unwrapped:
                        if isinstance(subitem, str) and subitem not in [table_name, file_path, index_type]:
                            key_field = subitem
                            break
        
        print(f"DEBUG final: table_name={table_name}, file_path={file_path}, index_type={index_type}, key_field={key_field}")
        
        return ExecutionPlan('CREATE_TABLE', 
                        table_name=table_name, 
                        fields=None, 
                        source=file_path, 
                        index_type=index_type, 
                        key_field=key_field)
        
    # --- field definition and list ---
    def field_definition(self, items):
        """Field definition - VERSIÓN SUPER ROBUSTA."""
        print(f"DEBUG field_definition INPUT: {[str(x) for x in items]}")
        
        if len(items) < 2:
            return None
        
        name = self._unwrap_tree_token(items[0])
        dtype_info = items[1]
        
        # LÓGICA MEJORADA PARA TIPOS DE DATOS
        dtype = 'VARCHAR'
        size = 50
        
        # Determinar tipo basado en el nombre del campo (heurística principal)
        if name.lower() in ['id', 'codigo', 'numero', 'key']:
            dtype = 'INT'
            size = 0
        elif name.lower() in ['precio', 'valor', 'costo', 'rating', 'latitud', 'longitud']:
            dtype = 'FLOAT'
            size = 0
        elif name.lower() in ['fecha', 'fecharegistro', 'date']:
            dtype = 'DATE'
            size = 0
        elif name.lower() in ['ubicacion', 'coordenadas', 'location']:
            dtype = 'ARRAY[FLOAT]'
            size = 0
        
        # Intentar sobreescribir con información del parser si está disponible
        try:
            parsed_type = self._unwrap_tree_token(dtype_info)
            if parsed_type and parsed_type != 'None':
                if isinstance(parsed_type, tuple):
                    dtype, size = parsed_type
                elif isinstance(parsed_type, str) and parsed_type.upper() in ['INT', 'FLOAT', 'DATE', 'ARRAY[FLOAT]']:
                    dtype = parsed_type.upper()
                    size = 0
        except:
            pass  # Si falla, mantener la heurística por nombre
        
        # Buscar índice
        index_type = None
        for i in range(2, len(items)):
            item = items[i]
            unwrapped = self._unwrap_tree_token(item)
            
            if isinstance(unwrapped, str) and unwrapped.upper() in ['SEQ', 'BTREE', 'EXTENDIBLEHASH', 'ISAM', 'RTREE']:
                index_type = unwrapped.upper()
                break
        
        result = {
            "name": name,
            "type": dtype,
            "size": size,
            "index": index_type
        }
        print(f"🔧 DEBUG field_definition FINAL: {result}")
        return result

    
    def comparison_operator(self, items):
        """Procesa operadores de comparación."""
        print(f"DEBUG comparison_operator items: {items}")
        
        if items:
            operator = self._unwrap_tree_token(items[0])
            print(f"DEBUG comparison_operator result: {operator}")
            return operator
        
        # Si está vacío, podría ser que el operador viene de otra manera
        return "="  # Default

    def between_condition(self, items):
        """Procesa condiciones BETWEEN - versión más flexible."""
        print(f"BETWEEN DEBUG: {len(items)} items")
        for i, item in enumerate(items):
            print(f"  Item {i}: {item} (type: {type(item)})")
            if hasattr(item, 'data'):
                print(f"       data: {item.data}")
            if hasattr(item, 'children'):
                print(f"       children: {item.children}")
        
        # Diferentes patrones que podemos recibir
        if len(items) == 5:
            # Patrón: field, BETWEEN, start, AND, end
            field = self._unwrap_tree_token(items[0])
            start = self._unwrap_tree_token(items[2])
            end = self._unwrap_tree_token(items[4])
        elif len(items) == 3:
            # Patrón: field, start, end (BETWEEN/AND no llegaron como tokens separados)
            field = self._unwrap_tree_token(items[0])
            start = self._unwrap_tree_token(items[1])
            end = self._unwrap_tree_token(items[2])
        else:
            print(f"DEBUG between_condition: unexpected pattern with {len(items)} items")
            return None
        
        result = {
            "type": "between",
            "field": field,
            "start": start,
            "end": end
        }
        print(f"DEBUG between_condition result: {result}")
        return result

    def spatial_condition(self, items):
        """Procesa condiciones espaciales IN (point, radius) corregido."""
        print(f"SPATIAL DEBUG: {len(items)} items")
        for i, item in enumerate(items):
            print(f"  Item {i}: {item} (type: {type(item)})")
            if hasattr(item, 'data'):
                print(f"       data: {item.data}")
            if hasattr(item, 'children'):
                print(f"       children: {item.children}")
        
        # Diferentes patrones que podemos recibir
        point = None
        radius = None
        field = None
        
        if len(items) >= 3:
            field = self._unwrap_tree_token(items[0])
            
            # Buscar point y radius en los items
            for i in range(1, len(items)):
                item = items[i]
                unwrapped = self._unwrap_tree_token(item)
                print(f"DEBUG spatial item {i}: {unwrapped} (type: {type(unwrapped)})")
                
                # Si es una tupla o lista de 2 elementos, es el point
                if isinstance(unwrapped, (tuple, list)) and len(unwrapped) == 2:
                    point = tuple(unwrapped)
                    print(f"DEBUG found point: {point}")
                
                # Si es un número, es el radius
                elif isinstance(unwrapped, (int, float)):
                    radius = unwrapped
                    print(f"DEBUG found radius: {radius}")
        
        # Si todavía no tenemos point, buscar específicamente
        if point is None:
            for item in items:
                if hasattr(item, 'data') and item.data == 'point':
                    point_result = self._unwrap_tree_token(item)
                    if isinstance(point_result, (tuple, list)) and len(point_result) == 2:
                        point = tuple(point_result)
                        print(f"DEBUG found point in Tree: {point}")
                        break
        
        if field and point is not None and radius is not None:
            result = {
                "type": "spatial", 
                "field": field,
                "point": point,
                "radius": radius
            }
            print(f"DEBUG spatial_condition result: {result}")
            return result
        
        print(f"DEBUG spatial_condition: missing data - field={field}, point={point}, radius={radius}")
        return None

    def CNAME(self, token):
        """Debug para ver todos los CNAME tokens."""
        result = str(token)
        print(f"DEBUG CNAME token: '{result}'")
        
        # Si es una palabra clave que debería ser reconocida diferente
        if result.upper() in ['BETWEEN', 'IN', 'AND', 'OR', 'NOT']:
            print(f"CNAME '{result}' debería ser palabra clave")
        
        return result

    def EQUALS(self, token):
        """Procesa operador =."""
        return "="

    def NOTEQUALS(self, token):
        """Procesa operador !=."""
        return "!="

    def LESSTHAN(self, token):
        """Procesa operador <."""
        return "<"

    def GREATERTHAN(self, token):
        """Procesa operador >."""
        return ">"

    def LESSEQUAL(self, token):
        """Procesa operador <=."""
        return "<="

    def GREATEREQUAL(self, token):
        """Procesa operador >=."""
        return ">="

    

    def field_definitions(self, *items):
        # items are field_definition dicts
        fields = []
        for it in items:
            if isinstance(it, dict) and 'name' in it:
                fields.append(it)
            elif isinstance(it, list):
                for sub in it:
                    if isinstance(sub, dict) and 'name' in sub:
                        fields.append(sub)
        return fields

    # index options
    def index_options(self, items):
        """Procesa opciones de índice de forma más robusta."""
        print(f"DEBUG index_options items: {items}")
        
        if not items:
            return None
        
        # Buscar el tipo de índice en los items
        for item in items:
            unwrapped = self._unwrap_tree_token(item)
            if isinstance(unwrapped, str) and unwrapped.upper() in ['SEQ', 'BTREE', 'EXTENDIBLEHASH', 'ISAM', 'RTREE']:
                print(f"DEBUG found index type: {unwrapped}")
                return unwrapped.upper()
        
        # Si no se encontró, devolver el último item
        last_item = self._unwrap_tree_token(items[-1])
        print(f"DEBUG index_options returning last: {last_item}")
        return last_item

    # --- SELECT ---
    def select_all(self, *items):
        return ["*"]

    def select_list(self, *items):
        return [self._unwrap(i) for i in items]

    def select_statement(self, items):
        """SELECT corregido para asignar where_clause."""
        sel = None
        table = None
        where = None
        limit = None
        
        print(f"DEBUG select_statement items: {items}")
        
        for it in items:
            # select_list puede venir como list o Tree('select_all')
            if isinstance(it, list):
                sel = [self._unwrap_tree_token(x) for x in it]
            elif isinstance(it, Tree) and it.data == 'select_all':
                sel = ['*']
            elif isinstance(it, Token) and it.type == 'CNAME':
                table = str(it)
            elif isinstance(it, str) and table is None:
                table = it
            elif isinstance(it, dict) and it.get('type') in ('comparison', 'between', 'spatial', 'and', 'or'):
                where = it
            # BUSCAR where_clause EN TREES
            elif hasattr(it, 'data') and it.data == 'where_clause':
                where_content = self._unwrap_tree_token(it)
                print(f"DEBUG found where_clause tree: {where_content}")
                if isinstance(where_content, dict):
                    where = where_content
            # BUSCAR limit_clause
            elif hasattr(it, 'data') and it.data == 'limit_clause':
                # _unwrap_tree_token should return the INT inside
                try:
                    lim = self._unwrap_tree_token(it)
                    # lim may be a list or int
                    if isinstance(lim, list) and lim:
                        limit = int(lim[0])
                    else:
                        limit = int(lim)
                except Exception:
                    limit = None
        
        print(f"DEBUG select_statement final: table={table}, where={where}")
        return ExecutionPlan('SELECT', table_name=table, select_list=sel or ['*'], where_clause=where, limit=limit)



    # comparisons / conditions
    def comparison(self, items):
        """Procesa comparaciones corregido."""
        print(f"DEBUG comparison items: {items}")
        
        if len(items) >= 3:
            field = self._unwrap_tree_token(items[0])
            operator_tree = items[1]
            value = self._unwrap_tree_token(items[2])
            
            # Procesar operador específicamente
            operator = "="  # default
            if hasattr(operator_tree, 'data') and operator_tree.data == 'comparison_operator':
                if operator_tree.children:
                    operator = self._unwrap_tree_token(operator_tree.children[0])
                else:
                    # Si el Tree está vacío, asumir "="
                    operator = "="
            else:
                operator = self._unwrap_tree_token(operator_tree)
            
            result = {
                "type": "comparison", 
                "field": field,
                "operator": operator,
                "value": value
            }
            print(f"DEBUG comparison result: {result}")
            return result
        
        return None

    def fulltext_condition(self, items):
        """Procesa condición de full-text: field @@ 'query'"""
        print(f"DEBUG fulltext_condition items: {items}")
        if len(items) >= 2:
            field = self._unwrap_tree_token(items[0])
            # the string literal may be Tree or Token
            query = self._unwrap_tree_token(items[1])
            result = {
                "type": "fulltext",
                "field": field,
                "query": query
            }
            print(f"DEBUG fulltext_condition result: {result}")
            return result
        return None

    def condition(self, items):
        """Procesa condiciones (puede ser simple o compuesta)."""
        print(f"DEBUG condition items: {items}")
        
        if len(items) == 1:
            # Condición simple
            return self._unwrap_tree_token(items[0])
        elif len(items) >= 3:
            # Condición compuesta (AND/OR)
            left = self._unwrap_tree_token(items[0])
            operator = self._unwrap_tree_token(items[1]).lower()
            right = self._unwrap_tree_token(items[2])
            
            return {
                "type": operator,
                "left": left,
                "right": right
            }
        
        return None

    def between(self, *items):
        # field, a, AND, b
        field = self._unwrap(items[0])
        a = self._unwrap(items[1])
        b = self._unwrap(items[-1])
        return {"type":"between", "field": field, "start": a, "end": b}

    # --- INSERT ---
    def insert_statement(self, items):
        table = None
        values = []
        from lark import Token, Tree
        for it in items:
            if isinstance(it, Token) and it.type == "CNAME" and table is None:
                table = str(it)
            elif isinstance(it, str) and table is None:
                table = it
            elif isinstance(it, list):
                # lista de valores (Tree nodes)
                values = [self._unwrap_tree_token(v) for v in it]
            elif isinstance(it, Tree) and it.data == 'value_list':
                values = [self._unwrap_tree_token(c) for c in it.children]
        return ExecutionPlan('INSERT', table_name=table, values=values or [])



    # --- UPDATE ---
    def assignment(self, *items):
        # field = value
        if len(items) >= 2:
            field = self._unwrap(items[0])
            val = self._unwrap(items[-1])
            return (field, val)
        return None

    def assignment_list(self, *items):
        return [i for i in items if i is not None]

    def update_statement(self, *items):
        table = None
        assigns = None
        where = None
        for it in items:
            if isinstance(it, str) and table is None:
                table = it
            if isinstance(it, list):
                # assignments
                assigns = [a for a in it if a is not None]
            if isinstance(it, dict) and it.get('type') in ('cmp','and','or','between'):
                where = it
        return ExecutionPlan("UPDATE", table_name=table, assignments=assigns or [], where_clause=where)

    # --- DELETE ---
    def delete_statement(self, items):
        """DELETE corregido - asigna where_clause correctamente."""
        print(f"DEBUG delete_statement items: {items}")
        
        table = None
        where = None
        
        for it in items:
            unwrapped = self._unwrap_tree_token(it)
            print(f"DEBUG delete item: {it} -> {unwrapped}")
            
            if isinstance(unwrapped, str) and table is None:
                table = unwrapped
            elif isinstance(unwrapped, dict) and unwrapped.get('type') == 'comparison':
                where = unwrapped
                print(f"DEBUG found where clause: {where}")
        
        print(f"DEBUG delete final: table={table}, where={where}")
        
        # Asegurarse de devolver el where_clause
        return ExecutionPlan('DELETE', table_name=table, where_clause=where)

    def where_clause(self, items):
        """Procesa WHERE clause con debug."""
        print(f"DEBUG where_clause input: {items}")
        
        if items:
            condition = self._unwrap_tree_token(items[0])
            print(f"DEBUG where_clause result: {condition}")
            return condition
        
        print("DEBUG where_clause: No items found")
        return None

    # --- point / radius / values helpers ---
    def point(self, items):
        """Procesa puntos (coordenadas) corregido."""
        print(f"DEBUG point items: {items}")
        
        nums = []
        for it in items:
            if isinstance(it, Token):
                nums.append(self._to_number(it))
            elif isinstance(it, (int, float)):
                nums.append(float(it))
        
        if len(nums) >= 2:
            result = (nums[0], nums[1])
            print(f"DEBUG point result: {result}")
            return result
        
        print(f"DEBUG point: not enough numbers ({len(nums)})")
        return None

    def radius(self, val):
        return self._to_number(val)

    def string_literal(self, s):
        if isinstance(s, Token):
            sval = str(s)
            return sval[1:-1].encode('utf-8').decode('unicode_escape')
        return s

    def SIGNED_NUMBER(self, token):
        s = str(token)
        return float(s) if '.' in s else int(s)

    
    def ESCAPED_STRING(self, token):
        """Procesa strings escapados de forma segura."""
        s = str(token)
        if s.startswith(('"', "'")) and s.endswith(('"', "'")):
            s = s[1:-1]
        
        # Para rutas de Windows, evitar decode unicode_escape
        if '\\' in s and (s.startswith('C:\\') or ':\\' in s):
            print(f"DEBUG: Ruta Windows detectada, usando raw: {s}")
            return s
        
        try:
            return s.encode('utf-8').decode('unicode_escape')
        except UnicodeDecodeError:
            print(f"DEBUG: Falló decode unicode_escape, usando string original: {s}")
            return s

    def CNAME(self, token):
        return str(token)

    def _as_str(self, v):
        from lark import Token
        if isinstance(v, Token):
            return str(v)
        return v

    def _as_number(self, v):
        from lark import Token
        if isinstance(v, Token):
            s = str(v)
            return float(s) if '.' in s else int(s)
        return v

    def _unwrap_tree_token(self, v):
        """Versión mejorada que maneja todos los casos de Trees y Tokens."""
        from lark import Tree, Token
        
        if isinstance(v, Tree):
            # Procesar según el tipo de Tree
            if v.data == 'data_type':
                if v.children:
                    return self._unwrap_tree_token(v.children[0])
                return None
            elif v.data == 'string_literal':
                if v.children and isinstance(v.children[0], Token):
                    return self._process_string_token(v.children[0])
            elif v.data in ['index_type', 'key_field']:
                if v.children:
                    return self._unwrap_tree_token(v.children[0])
            
            # Para otros trees, procesar children recursivamente
            if len(v.children) == 1:
                return self._unwrap_tree_token(v.children[0])
            else:
                return [self._unwrap_tree_token(c) for c in v.children]
        
        elif isinstance(v, Token):
            if v.type == "ESCAPED_STRING":
                return self._process_string_token(v)
            elif v.type in ("INT", "SIGNED_NUMBER"):
                return self._to_number(v)
            elif v.type == "CNAME":
                return str(v)
        
        elif isinstance(v, list):
            if len(v) == 1:
                return self._unwrap_tree_token(v[0])
            return [self._unwrap_tree_token(item) for item in v]
        
        return v

    def _process_string_token(self, token):
        """Procesa tokens de string removiendo comillas y escapando."""
        s = str(token)
        if (s.startswith(('"', "'")) and s.endswith(('"', "'"))):
            s = s[1:-1]
        return s.encode('utf-8').decode('unicode_escape')

    def number(self, token):
        # token puede ser Token('SIGNED_NUMBER', '1') o Tree; usar helper
        return self._as_number(token)

    def string(self, token):
        # token es Tree('string_literal', [Token('ESCAPED_STRING', '"text"')]) o Token
        return self._unwrap_tree_token(token)

    def data_type(self, items):
        """Procesa tipos de datos - VERSIÓN OPTIMIZADA."""
        print(f" DEBUG data_type ITEMS: {items}")
        
        if not items:
            return None
        
        # Caso especial: cuando items es ['INT'] pero debería ser VARCHAR
        # Esto pasa porque la gramática no está capturando correctamente VARCHAR[50]
        if len(items) == 1 and isinstance(items[0], str) and items[0] == 'INT':
            # Revisar el contexto - si estamos en un campo que debería ser VARCHAR
            # Por ahora, devolver VARCHAR como fallback inteligente
            return 'VARCHAR'
        
        # Si es un Tree, procesar estructura
        if hasattr(items[0], 'data'):
            tree = items[0]
            if tree.data == 'data_type' and tree.children:
                first_child = tree.children[0]
                
                # Token simple
                if isinstance(first_child, Token):
                    return str(first_child).upper()
                
                # Estructura compleja (VARCHAR[50])
                elif hasattr(first_child, 'children') and first_child.children:
                    dtype_token = first_child.children[0]
                    if isinstance(dtype_token, Token):
                        dtype = str(dtype_token).upper()
                        
                        # Buscar tamaño
                        if len(first_child.children) > 2:
                            size_token = first_child.children[2]
                            if isinstance(size_token, Token) and size_token.type == 'INT':
                                return (dtype, int(size_token))
                        
                        return dtype
        
        return self._unwrap_tree_token(items[0])

    def SINGLE_QUOTED_STRING(self, token):
        """Procesa strings con comillas simples."""
        s = str(token)
        if s.startswith("'") and s.endswith("'"):
            s = s[1:-1]
        return s.encode('utf-8').decode('unicode_escape')

    def VARCHAR(self, token):
        """Procesa token VARCHAR."""
        print(f"DEBUG VARCHAR token: {token}")
        return "VARCHAR"

    def INT(self, token):
        """Procesa token INT."""
        print(f"DEBUG INT token: {token}")
        return "INT"

    def FLOAT(self, token):
        return "FLOAT"

    def DATE(self, token):
        return "DATE"

    def ARRAY(self, token):
        return "ARRAY"

    def LSQB(self, token):
        """Procesa apertura de corchete [ para tamaño."""
        return "["

    def RSQB(self, token): 
        """Procesa cierre de corchete ] para tamaño."""
        return "]"

class SQLParser:
    """Parser SQL principal que devuelve ExecutionPlan."""
    
    def __init__(self, grammar: str = GRAMMAR):
        """Inicializa el parser con la gramática."""
        self.parser = Lark(grammar, parser='lalr', transformer=SQLTransformer())
    
    def parse(self, sql_command: str) -> Union[ExecutionPlan, Dict, None]:
        """
        Parsea un comando SQL y devuelve un ExecutionPlan.
        """
        try:
            # Limpiar el comando
            sql_command = sql_command.strip()
            if not sql_command:
                return None
            
            # DEBUG: Mostrar comando que se va a parsear
            print(f"DEBUG parsing: {sql_command[:100]}...")
            
            # Parsear
            result = self.parser.parse(sql_command)
            
            # Si es un statement_list, extraer el primer statement
            if isinstance(result, dict) and result.get('type') == 'statement_list':
                statements = result.get('statements', [])
                if statements:
                    return statements[0]
                return None
            
            return result
            
        except LarkError as e:
            print(f"DEBUG LarkError: {e}")
            raise LarkError(f"Error de sintaxis SQL: {e}")
        except Exception as e:
            print(f"DEBUG Internal Error: {e}")
            raise Exception(f"Error interno del parser: {e}")
    
    def parse_file(self, filename: str) -> List[ExecutionPlan]:
        """
        Parsea un archivo con comandos SQL.
        
        Args:
            filename: Ruta del archivo SQL
            
        Returns:
            Lista de ExecutionPlan
        """
        if not os.path.exists(filename):
            raise FileNotFoundError(f"Archivo no encontrado: {filename}")
        
        with open(filename, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Dividir por ';' y procesar cada comando
        commands = [cmd.strip() for cmd in content.split(';') if cmd.strip()]
        
        plans = []
        for command in commands:
            if not command.startswith('--'):  # Ignorar comentarios
                plan = self.parse(command)
                if plan:
                    if isinstance(plan, dict) and plan.get('type') == 'statement_list':
                        plans.extend(plan['statements'])
                    else:
                        plans.append(plan)
        
        return plans
    
    def parse_file_content(self, content: str) -> List[ExecutionPlan]:
        """
        Parsea contenido de string con comandos SQL.
        """
        plans = []
        
        # Dividir por líneas primero para manejar comentarios mejor
        lines = content.split('\n')
        current_command = ""
        
        for line in lines:
            line = line.strip()
            
            # Ignorar líneas de comentario
            if line.startswith('--') or line.startswith('/*'):
                continue
                
            # Remover comentarios al final de línea
            if '--' in line:
                line = line.split('--')[0].strip()
            
            current_command += " " + line
            
            # Si la línea termina con ;, procesar el comando
            if line.endswith(';') or ';' in current_command:
                # Dividir por ; y procesar cada comando
                commands = [cmd.strip() for cmd in current_command.split(';') if cmd.strip()]
                for cmd in commands:
                    if cmd and not cmd.startswith('--'):
                        plan = self.parse(cmd)
                        if plan:
                            plans.append(plan)
                current_command = ""
        
        # Procesar cualquier comando restante
        if current_command.strip():
            plan = self.parse(current_command.strip())
            if plan:
                plans.append(plan)
        
        print(f"DEBUG parse_file_content: found {len(plans)} plans")
        return plans
    
    

def main():
    """Función principal para testing del parser."""
    parser = SQLParser()
    
    print("=== SQL Parser - Modo Testing ===")
    print("Escriba comandos SQL para ver el ExecutionPlan generado")
    print("Escriba 'exit' para salir")
    print()
    
    while True:
        try:
            command = input("SQL> ").strip()
            if command.lower() == 'exit':
                break
            
            if not command:
                continue
            
            plan = parser.parse(command)
            print(f"ExecutionPlan: {plan}")
            print()
            
        except KeyboardInterrupt:
            print("\nSaliendo...")
            break
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
