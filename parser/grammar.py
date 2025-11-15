GRAMMAR = r"""
start: statement_list?

statement_list: statement (";" statement)* ";"?    // permite múltiples statements, ; opcional al final

?statement: create_table_statement
          | create_table_from_file
          | select_statement
          | insert_statement
          | update_statement
          | delete_statement

// CREATE TABLE (schema)
create_table_statement: "CREATE"i "TABLE"i CNAME "(" field_definitions ")"

// CREATE TABLE ... FROM FILE ... USING INDEX ...
//create_from_file_statement: "CREATE"i "TABLE"i CNAME "FROM"i "FILE"i string_literal "USING"i "INDEX"i "(" CNAME ")"

create_table_from_file: "CREATE"i "TABLE"i CNAME "FROM"i "FILE"i string_literal "USING"i "INDEX"i index_type "(" key_field ")"
index_type: CNAME
key_field: CNAME | string_literal

field_definitions: field_definition ("," field_definition)*
field_definition: CNAME data_type index_options?

index_options: "KEY"i "INDEX"i index_type
             | "INDEX"i index_type

// DATA TYPES
data_type: "INT"i
         | "INTEGER"i  
         | "FLOAT"i
         | "DOUBLE"i
         | "DATE"i
         | "VARCHAR"i "[" INT "]"   // VARCHAR[50] - SIN regla especial
         | "STRING"i "[" INT "]"    // STRING[50] - SIN regla especial  
         | "ARRAY"i "[" "FLOAT"i "]" // ARRAY[FLOAT] - SIN regla especial


// INDEX TYPES (aceptamos variantes en transformer)
//index_type: CNAME
//key_field: CNAME | string_literal

// SELECT
select_statement: "SELECT"i select_list "FROM"i CNAME where_clause? order_clause? limit_clause?
select_list: "*" -> select_all
           | field_name ("," field_name)*

// WHERE clause mejorado
where_clause: "WHERE"i condition

?condition: comparison 
          | between_condition
          | fulltext_condition
          | spatial_condition
          | condition ("AND"i | "OR"i) condition

// Usar patrones más simples
between_condition: field_name "BETWEEN"i value "AND"i value
spatial_condition: field_name "IN"i "(" value "," value ")"  // point y radius como values genéricos

comparison: field_name comparison_operator value
comparison_operator: "=" | "!=" | "<>" | "<" | ">" | "<=" | ">="

// Full-text operator (SQL-like @@)
fulltext_condition: field_name "@@" string_literal

// Optional ORDER BY / LIMIT
order_clause: "ORDER"i "BY"i field_name ("ASC"i | "DESC"i)?
limit_clause: "LIMIT"i INT

// INSERT
insert_statement: "INSERT"i "INTO"i CNAME "VALUES"i "(" value_list ")"
value_list: value ("," value)*

// UPDATE
update_statement: "UPDATE"i CNAME "SET"i assignment_list where_clause?
assignment_list: assignment ("," assignment)*
assignment: field_name "=" value

// DELETE
delete_statement: "DELETE"i "FROM"i CNAME where_clause?

// VALUES and point/radius
?value: SIGNED_NUMBER     -> number
      | string_literal    -> string
      | point
      | "NULL"i           -> null

point: "(" SIGNED_NUMBER "," SIGNED_NUMBER ")"
radius: SIGNED_NUMBER

// Identifiers and literals
table_name: CNAME
field_name: CNAME

string_literal: ESCAPED_STRING | SINGLE_QUOTED_STRING

// Tokens: rely on common tokens from lark
%import common.CNAME
%import common.ESCAPED_STRING
%import common.SIGNED_NUMBER
%import common.INT
%import common.WS
%ignore WS

// Comments
%import common.C_COMMENT
%import common.CPP_COMMENT
%ignore C_COMMENT
%ignore CPP_COMMENT

// Agregar soporte para comillas simples
SINGLE_QUOTED_STRING: /'[^']*'/
"""
