/*******************************************************************************************

BNF Grammar for BQL
===================

<statement> ::= ( <select_stmt> | <describe_stmt> ) [';']

<select_stmt> ::= SELECT <select_list> [<from_clause>] [<where_clause>] [<given_clause>]
                  [<additional_clauses>]

<describe_stmt> ::= ( DESC | DESCRIBE ) [<index_name>]

<select_list> ::= '*' | (<column_name>|<aggregation_function>)( ',' <column_name>|<aggregation_function> )*

<column_name_list> ::= <column_name> ( ',' <column_name> )*

<aggregation_function> ::= <function_name> '(' <column_name> ')'

<function_name> ::= <column_name>

<from_clause> ::= FROM <index_name>

<index_name> ::= <identifier> | <quoted_string>

<where_clause> ::= WHERE <search_expr>

<search_expr> ::= <term_expr> ( OR <term_expr> )*

<term_expr> ::= <facet_expr> ( AND <facet_expr> )*

<facet_expr> ::= <predicate> 
               | '(' <search_expr> ')'

<predicates> ::= <predicate> ( AND <predicate> )*

<predicate> ::= <in_predicate>
              | <contains_all_predicate>
              | <equal_predicate>
              | <not_equal_predicate>
              | <query_predicate>
              | <between_predicate>
              | <range_predicate>
              | <time_predicate>
              | <match_predicate>
              | <like_predicate>
              | <null_predicate>

<in_predicate> ::= <column_name> [NOT] IN <value_list> [<except_clause>] [<predicate_props>]

<contains_all_predicate> ::= <column_name> CONTAINS ALL <value_list> [<except_clause>]
                             [<predicate_props>]

<equal_predicate> ::= <column_name> '=' <value> [<predicate_props>]

<not_equal_predicate> ::= <column_name> '<>' <value> [<predicate_props>]

<query_predicate> ::= QUERY IS <quoted_string>

<between_predicate> ::= <column_name> [NOT] BETWEEN <value> AND <value>

<range_predicate> ::= <column_name> <range_op> <numeric>

<time_predicate> ::= <column_name> IN LAST <time_span>
                   | <column_name> ( SINCE | AFTER | BEFORE ) <time_expr>

<match_predicate> ::= [NOT] MATCH '(' <column_name_list> ')' AGAINST '(' <quoted_string> ')'

<like_predicate> ::= <column_name> [NOT] LIKE <quoted_string>

<null_predicate> ::= <column_name> IS [NOT] NULL

<value_list> ::= <non_variable_value_list> | <variable>

<non_variable_value_list> ::= '(' <value> ( ',' <value> )* ')'

<python_style_list> ::= '[' <python_style_value>? ( ',' <python_style_value> )* ']'

<python_style_dict> ::= '{''}' 
                       | '{' <key_value_pair> ( ',' <key_value_pair> )* '}'

<python_style_value> ::= <value>
                       | <python_style_list>
                       | <python_style_dict>

<value> ::= <numeric>
          | <quoted_string>
          | TRUE
          | FALSE
          | <variable>

<range_op> ::= '<' | '<=' | '>=' | '>'

<except_clause> ::= EXCEPT <value_list>

<predicate_props> ::= WITH <prop_list>

<prop_list> ::= '(' <key_value_pair> ( ',' <key_value_pair> )* ')'

<key_value_pair> ::= <quoted_string> ':' 
                     ( <value> | <python_style_list> | <python_style_dict> )

<given_clause> ::= GIVEN FACET PARAM <facet_param_list>

<facet_param_list> ::= <facet_param> ( ',' <facet_param> )*

<facet_param> ::= '(' <facet_name> <facet_param_name> <facet_param_type> <facet_param_value> ')'

<facet_param_name> ::= <quoted_string>

<facet_param_type> ::= BOOLEAN | INT | LONG | STRING | BYTEARRAY | DOUBLE

<facet_param_value> ::= <quoted_string>

<additional_clauses> ::= ( <additional_clause> )+

<additional_clause> ::= <order_by_clause>
                      | <limit_clause>
                      | <group_by_clause>
                      | <distinct_clause>
                      | <execute_clause>
                      | <browse_by_clause>
                      | <fetching_stored_clause>
                      | <route_by_clause>
                      | <relevance_model_clause>

<order_by_clause> ::= ORDER BY <sort_specs>

<sort_specs> ::= <sort_spec> ( ',' <sort_spec> )*

<sort_spec> ::= <column_name> [<ordering_spec>]

<ordering_spec> ::= ASC | DESC

<group_by_clause> ::= GROUP BY <group_spec>

<distinct_clause> ::= DISTINCT <distinct_spec>

<execute_clause> ::= EXECUTE '(' function_name ((',' python_style_dict) | (',' key_value_pair)*) ')'

<group_spec> ::= <comma_column_name_list> [TOP <max_per_group>]

<distinct_spec> ::= <or_column_name_list>

<or_column_name_list> ::= <column_name> ( OR <column_name> )*

<comma_column_name_list> ::= <column_name> ( (OR | ',') <column_name> )*

<limit_clause> ::= LIMIT [<offset> ','] <count>

<offset> ::= ( <digit> )+

<count> ::= ( <digit> )+

<browse_by_clause> ::= BROWSE BY <facet_specs>

<facet_specs> ::= <facet_spec> ( ',' <facet_spec> )*

<facet_spec> ::= <facet_name> [<facet_expression>]

<facet_expression> ::= '(' <expand_flag> <count> <count> <facet_ordering> ')'

<expand_flag> ::= TRUE | FALSE

<facet_ordering> ::= HITS | VALUE

<fetching_stored_clause> ::= FETCHING STORED [<fetching_flag>]

<fetching_flag> ::= TRUE | FALSE

<route_by_clause> ::= ROUTE BY <quoted_string>

<relevance_model_clause> ::= USING RELEVANCE MODEL <identifier> <prop_list>
                             [<relevance_model>]

<relevance_model> ::= DEFINED AS <formal_parameters> BEGIN <model_block> END

<formal_parameters> ::= '(' <formal_parameter_decls> ')'

<formal_parameter_decls> ::= <formal_parameter_decl> ( ',' <formal_parameter_decl> )*

<formal_parameter_decl> ::= <variable_modifiers> <type> <variable_declarator_id>

<variable_modifiers> ::= ( <variable_modifier> )*

<variable_modifier> ::= 'final'

<type> ::= <class_or_interface_type> ('[' ']')*
         | <primitive_type> ('[' ']')*
         | <boxed_type> ('[' ']')*
         | <limited_type> ('[' ']')*

<class_or_interface_type> ::= <fast_util_data_type>

<fast_util_data_type> ::= 'IntOpenHashSet'
                        | 'FloatOpenHashSet'
                        | 'DoubleOpenHashSet'
                        | 'LongOpenHashSet'
                        | 'ObjectOpenHashSet'
                        | 'Int2IntOpenHashMap'
                        | 'Int2FloatOpenHashMap'
                        | 'Int2DoubleOpenHashMap'
                        | 'Int2LongOpenHashMap'
                        | 'Int2ObjectOpenHashMap'
                        | 'Object2IntOpenHashMap'
                        | 'Object2FloatOpenHashMap'
                        | 'Object2DoubleOpenHashMap'
                        | 'Object2LongOpenHashMap'
                        | 'Object2ObjectOpenHashMap'

<primitive_type> ::= 'boolean' | 'char' | 'byte' | 'short' 
                   | 'int' | 'long' | 'float' | 'double'

<boxed_type> ::= 'Boolean' | 'Character' | 'Byte' | 'Short' 
               | 'Integer' | 'Long' | 'Float' | 'Double'

<limited_type> ::= 'String' | 'System' | 'Math'

<model_block> ::= ( <block_statement> )+

<block_statement> ::= <local_variable_declaration_stmt>
                    | <java_statement>

<local_variable_declaration_stmt> ::= <local_variable_declaration> ';'

<local_variable_declaration> ::= <variable_modifiers> <type> <variable_declarators>

<java_statement> ::= <block>
                   | 'if' <par_expression> <java_statement> [ <else_statement> ]
                   | 'for' '(' <for_control> ')' <java_statement>
                   | 'while' <par_expression> <java_statement>
                   | 'do' <java_statement> 'while> <par_expression> ';'
                   | 'switch' <par_expression> '{' <switch_block_statement_groups> '}'
                   | 'return' <expression> ';'
                   | 'break' [<identifier>] ';'
                   | 'continue' [<identifier>] ';'
                   | ';'
                   | <statement_expression> ';'

<block> ::= '{' ( <block_statement> )* '}'

<else_statement> ::= 'else' <java_statement>

<switch_block_statement_groups> ::= ( <switch_block_statement_group> )*

<switch_block_statement_group> ::= ( <switch_label> )+ ( <block_statement> )*

<switch_label> ::= 'case' <constant_expression> ':'
                 | 'case' <enum_constant_name> ':'
                 | 'default' ':'

<for_control> ::= <enhanced_for_control>
                | [<for_init>] ';' [<expression>] ';' [<for_update>]

<for_init> ::= <local_variable_declaration>
             | <expression_list>

<enhanced_for_control> ::= <variable_modifiers> <type> <identifier> ':' <expression>

<for_update> ::= <expression_list>

<par_expression> ::= '(' <expression> ')'

<expression_list> ::= <expression> ( ',' <expression> )*

<statement_expression> ::= <expression>

<constant_expression> ::= <expression>

<enum_constant_name> ::= <identifier>

<variable_declarators> ::= <variable_declarator> ( ',' <variable_declarator> )*

<variable_declarator> ::= <variable_declarator_id> '=' <variable_initializer>

<variable_declarator_id> ::= <identifier> ('[' ']')*

<variable_initializer> ::= <array_initializer>
                         | <expression>

<array_initializer> ::= '{' [ <variable_initializer> ( ',' <variable_initializer> )* [','] ] '}'

<expression> ::= <conditional_expression> [ <assignment_operator> <expression> ]

<assignment_operator> ::= '=' | '+=' | '-=' | '*=' | '/=' | '&=' | '|=' | '^=' |
                        | '%=' | '<<=' | '>>>=' | '>>='

<conditional_expression> ::= <conditional_or_expression> [ '?' <expression> ':' <expression> ]

<conditional_or_expression> ::= <conditional_and_expression> ( '||' <conditional_and_expression> )*

<conditional_and_expression> ::= <inclusive_or_expression> ('&&' <inclusive_or_expression> )*

<inclusive_or_expression> ::= <exclusive_or_expression> ('|' <exclusive_or_expression> )*

<exclusive_or_expression> ::= <and_expression> ('^' <and_expression> )*

<and_expression> ::= <equality_expression> ( '&' <equality_expression> )*

<equality_expression> ::= <instanceof_expression> ( ('==' | '!=') <instanceof_expression> )*

<instanceof_expression> ::= <relational_expression> [ 'instanceof' <type> ]

<relational_expression> ::= <shift_expression> ( <relational_op> <shift_expression> )*

<shift_expression> ::= <additive_expression> ( <shift_op> <additive_expression> )*

<relational_op> ::= '<=' | '>=' | '<' | '>'

<shift_op> ::= '<<' | '>>>' | '>>'

<additive_expression> ::= <multiplicative_expression> ( ('+' | '-') <multiplicative_expression> )*

<multiplicative_expression> ::= <unary_expression> ( ( '*' | '/' | '%' ) <unary_expression> )*

<unary_expression> ::= '+' <unary_expression>
                     | '-' <unary_expression>
                     | '++' <unary_expression>
                     | '--' <unary_expression>
                     | <unary_expression_not_plus_minus>

<unary_expression_not_plus_minus> ::= '~' <unary_expression>
                                    | '!' <unary_expression>
                                    | <cast_expression>
                                    | <primary> <selector>* [ ('++'|'--') ]

<cast_expression> ::= '(' <primitive_type> ')' <unary_expression>
                    | '(' (<type> | <expression>) ')' <unary_expression_not_plus_minus>

<primary> ::= <par_expression>
            | <literal>
            | java_method identifier_suffix
            | <java_ident> ('.' <java_method>)* [<identifier_suffix>]

<java_ident> ::= <boxed_type>
               | <limited_type>
               | <identifier>

<java_method> ::= <identifier>

<identifier_suffix> ::= ('[' ']')+ '.' 'class'
                      | <arguments>
                      | '.' 'class'
                      | '.' 'this'
                      | '.' 'super' <arguments>

<literal> ::= <integer>
            | <real>
            | <floating_point_literal>
            | <character_literal>
            | <quoted_string>
            | <boolean_literal>
            | 'null'

<boolean_literal> ::= 'true' | 'false'

<selector> ::= '.' <identifier> <arguments>
             | '.' 'this'
             | '[' <expression> ']'

<arguments> ::= '(' [<expression_list>] ')'

<quoted_string> ::= '"' ( <char> )* '"'
                  | "'" ( <char> )* "'"

<identifier> ::= <identifier_start> ( <identifier_part> )*

<identifier_start> ::= <alpha> | '-' | '_'

<identifier_part> ::= <identifier_start> | <digit>

<variable> ::= '$' ( <alpha> | <digit> | '_' )+

<column_name> ::= <identifier> | <quoted_string>

<facet_name> ::= <identifier>

<alpha> ::= <alpha_lower_case> | <alpha_upper_case>

<alpha_upper_case> ::= A | B | C | D | E | F | G | H | I | J | K | L | M | N | O
                     | P | Q | R | S | T | U | V | W | X | Y | Z

<alpha_lower_case> ::= a | b | c | d | e | f | g | h | i | j | k | l | m | n | o
                     | p | q | r | s | t | u | v | w | x | y | z

<digit> ::= 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8 | 9

<numeric> ::= <time_expr> 
            | <integer>
            | <real>

<integer> ::= ( <digit> )+

<real> ::= ( <digit> )+ '.' ( <digit> )+

<time_expr> ::= <time_span> AGO
              | <date_time_string>
              | NOW

<time_span> ::= [<time_week_part>] [<time_day_part>] [<time_hour_part>]
                [<time_minute_part>] [<time_second_part>] [<time_millisecond_part>]

<time_week_part> ::= <integer> ( 'week' | 'weeks' )

<time_day_part>  ::= <integer> ( 'day'  | 'days' )

<time_hour_part> ::= <integer> ( 'hour' | 'hours' )

<time_minute_part> ::= <integer> ( 'minute' | 'minutes' | 'min' | 'mins')

<time_second_part> ::= <integer> ( 'second' | 'seconds' | 'sec' | 'secs')

<time_millisecond_part> ::= <integer> ( 'millisecond' | 'milliseconds' | 'msec' | 'msecs')

<date_time_string> ::= <date> [<time>]

<date> ::= <digit><digit><digit><digit> ('-' | '/' | '.') <digit><digit>
           ('-' | '/' | '.') <digit><digit>

<time> ::= DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT

*******************************************************************************************/

grammar BQL;

// Imaginary tokens
tokens 
{
    COLUMN_LIST,
    OR_PRED,
    AND_PRED,
    EQUAL_PRED,
    RANGE_PRED
}

// As the generated lexer will reside in com.senseidb.bql.parsers package,
// we have to add package declaration on top of it
@lexer::header {
}

@lexer::members {

  // @Override
  // public void reportError(RecognitionException e) {
  //   throw new IllegalArgumentException(e);
  // }

}

// As the generated parser will reside in com.senseidb.bql.parsers
// package, we have to add package declaration on top of it
@parser::header {
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;
import com.senseidb.util.Pair;
import com.senseidb.search.req.BQLParserUtils;
}

@parser::members {

    private static enum KeyType {
      STRING_LITERAL,
      IDENT,
      STRING_LITERAL_AND_IDENT
    }

    //@Override
    //public String getErrorMessage(RecognitionException err, String[] tokenNames) 
    //{
    //    List stack = getRuleInvocationStack(err, this.getClass().getName());
    //    String msg = null; 
    //    if (err instanceof NoViableAltException) {
    //        NoViableAltException nvae = (NoViableAltException) err;
    //        // msg = "No viable alt; token=" + err.token.getText() +
    //        //     " (decision=" + nvae.decisionNumber +
    //        //     " state "+nvae.stateNumber+")" +
    //        //     " decision=<<" + nvae.grammarDecisionDescription + ">>";
    //        msg = "[line:" + err.line + ", col:" + err.charPositionInLine + "] " +
    //            "No viable alternative (token=" + err.token.getText() + ")" + " (stack=" + stack + ")";
    //    }
    //    else if (err instanceof MismatchedTokenException) {
    //        MismatchedTokenException mte = (MismatchedTokenException) err;
    //        String tokenName = (mte.expecting == Token.EOF) ? "EOF" : tokenNames[mte.expecting];
    //        msg = "[line:" + mte.line + ", col:" + mte.charPositionInLine + "] " +
    //            "Expecting " + tokenName +
    //            " (token=" + err.token.getText() + ")";
    //    }
    //    else if (err instanceof FailedPredicateException) {
    //        FailedPredicateException fpe = (FailedPredicateException) err;
    //        msg = "[line:" + fpe.line + ", col:" + fpe.charPositionInLine + "] " +
    //            fpe.predicateText +
    //            " (token=" + fpe.token.getText() + ")";
    //    }
    //    else if (err instanceof MismatchedSetException) {
    //        MismatchedSetException mse = (MismatchedSetException) err;
    //        msg = "[line:" + mse.line + ", col:" + mse.charPositionInLine + "] " +
    //            "Mismatched input (token=" + mse.token.getText() + ")";
    //    }
    //    else {
    //        msg = super.getErrorMessage(err, tokenNames); 
    //    }
    //    return msg;
    //} 

    //@Override
    //public String getTokenErrorDisplay(Token t)
    //{
    //    return t.toString();
    //}
}

// ***************** parser rules:

statement
    :   (   select_stmt
        |   describe_stmt
        )   SEMI? EOF
    ;

select_stmt
    :   SELECT ('*' | cols=selection_list)
        (FROM (IDENT | STRING_LITERAL))?
        w=where?
        given=given_clause?
        (   order_by = order_by_clause 
        |   limit = limit_clause
        |   group_by = group_by_clause
        |   distinct = distinct_clause
        |   executeMapReduce = execute_clause
        |   browse_by = browse_by_clause
        |   fetch_stored = fetching_stored_clause
        |   route_param = route_by_clause
        |   rel_model = relevance_model_clause
        )*
    ;

describe_stmt
    :   DESCRIBE (IDENT | STRING_LITERAL)
    ;

selection_list
    :   (   col=column_name
        |   agrFunction=aggregation_function 
        )
        (   COMMA
            (   col=column_name
            |   agrFunction=aggregation_function
            )
        )*
    ;

aggregation_function
 :   (id=function_name LPAR (columnVar=column_name | '*') RPAR)
 ;

column_name
    :   (id=IDENT | str=STRING_LITERAL)
        ('.' (id2=IDENT | str2=STRING_LITERAL)
        )*
    ;

function_name
    :   (min= 'min'|colName=column_name)
    ;

where
    :   WHERE search_expr
    ;

order_by_clause
    :   ORDER BY (RELEVANCE | sort_specs)
    ;

sort_specs
    :   sort=sort_spec
        (COMMA sort=sort_spec   // It's OK to use variable sort again here
        )*
    ;

sort_spec
    :   column_name ordering=(ASC | DESC)?
    ;

limit_clause
    :   LIMIT (n1=INTEGER COMMA)? n2=INTEGER
    ;

comma_column_name_list
    :   col=column_name
        ((OR | COMMA) col=column_name
        )*
    ;

or_column_name_list
    :   col=column_name
        (OR col=column_name
        )*
    ;

group_by_clause
    :   GROUP BY comma_column_name_list (TOP top=INTEGER)?
    ;

distinct_clause
    :   DISTINCT or_column_name_list
    ;

browse_by_clause
    :   BROWSE BY f=facet_spec
        (COMMA f=facet_spec
        )*
    ;

execute_clause
    :   EXECUTE LPAR funName=function_name
        (   COMMA map=python_style_dict
        |   (   COMMA p=key_value_pair[KeyType.STRING_LITERAL]
            )*
        )
        RPAR 
    ;

facet_spec
    :   column_name
        (
            LPAR 
            (TRUE | FALSE) COMMA
            n1=INTEGER COMMA
            n2=INTEGER COMMA
            (HITS | VALUE)
            RPAR
        )*
    ;

fetching_stored_clause
    :   FETCHING STORED
        (   TRUE
        |   FALSE
        )*
    ;

route_by_clause
    :   ROUTE BY STRING_LITERAL 
    ;

search_expr
    :   t=term_expr
        (OR t=term_expr)*
    ;

term_expr
    :   f=factor_expr
        (AND f=factor_expr)*
    ;

factor_expr
    :   predicate
    |   LPAR search_expr RPAR
    ;

predicate
    :   in_predicate
    |   contains_all_predicate
    |   equal_predicate
    |   not_equal_predicate
    |   query_predicate
    |   between_predicate
    |   range_predicate
    |   time_predicate
    |   match_predicate
    |   like_predicate
    |   null_predicate
    |   empty_predicate
    ;

in_predicate
    :   column_name not=NOT? IN value_list except=except_clause? predicate_props?
    ;

empty_predicate
    :   value_list IS (NOT)? EMPTY
    ;
    
contains_all_predicate
    :   column_name CONTAINS ALL value_list except=except_clause? predicate_props? 
    ;

equal_predicate
    :   column_name EQUAL value props=predicate_props?
    ;

not_equal_predicate
    :   column_name NOT_EQUAL value predicate_props?
    ;

query_predicate
    :   QUERY IS STRING_LITERAL
    ;

between_predicate
    :   column_name not=NOT? BETWEEN val1=value AND val2=value
    ;

range_predicate
    :   column_name op=(GT | GTE | LT | LTE) val=value
    ;

time_predicate
    :   column_name (NOT)? IN LAST time_span
    |   column_name (NOT)? (since=SINCE | since=AFTER | before=BEFORE) time_expr
    ;

time_span
    :   week=time_week_part? day=time_day_part? hour=time_hour_part? 
        minute=time_minute_part? second=time_second_part? msec=time_millisecond_part?
    ;

time_week_part
    :   INTEGER WEEKS
    ;

time_day_part
    :   INTEGER DAYS
    ;

time_hour_part
    :   INTEGER HOURS
    ;

time_minute_part
    :   INTEGER (MINUTES | MINS)
    ;

time_second_part
    :   INTEGER (SECONDS | SECS)
    ;

time_millisecond_part
    :   INTEGER (MILLISECONDS | MSECS)
    ;

time_expr
    :   time_span AGO
    |   date_time_string
    |   NOW
    ;

date_time_string
    :   DATE TIME?
    ;

match_predicate
    :   (NOT)? MATCH LPAR selection_list RPAR AGAINST LPAR STRING_LITERAL RPAR
    ;

like_predicate
    :   column_name (NOT)? LIKE STRING_LITERAL
    ;

null_predicate
    :   column_name IS (NOT)? NULL
    ;

non_variable_value_list
    :   LPAR v=value
        (   COMMA v=value
        )*
        RPAR
    |   LPAR RPAR
    ;

python_style_list
    :   '[' v=python_style_value?
        (   COMMA v=python_style_value
        )*
        ']'
    ;

python_style_dict
    :   '{' '}'
    |   '{' p=key_value_pair[KeyType.STRING_LITERAL]
        (   COMMA p=key_value_pair[KeyType.STRING_LITERAL]
        )*
        '}'
    ;

python_style_value
    :   value
    |   python_style_list
    |   python_style_dict
    ;

value_list
    :   non_variable_value_list
    |   VARIABLE
    ;

value
    :   numeric
    |   STRING_LITERAL
    |   TRUE
    |   FALSE
    |   VARIABLE
    ;

numeric
    :   time_expr
    |   INTEGER
    |   REAL
    ;

except_clause
    :   EXCEPT value_list
    ;
  
predicate_props
    :   WITH prop_list[KeyType.STRING_LITERAL]
    ;

prop_list[KeyType keyType]
    :   LPAR p=key_value_pair[keyType]
        (   COMMA p=key_value_pair[keyType]
        )*
        RPAR
    ;

key_value_pair[KeyType keyType]
    :   ( { $keyType == KeyType.STRING_LITERAL ||
            $keyType == KeyType.STRING_LITERAL_AND_IDENT}? STRING_LITERAL
        | { $keyType == KeyType.IDENT ||
            $keyType == KeyType.STRING_LITERAL_AND_IDENT}? IDENT
        )
        COLON (v=value | vs=python_style_list | vd=python_style_dict)
    ;

given_clause
    :   GIVEN FACET PARAM facet_param_list
    ;


// =====================================================================
// Relevance model related
// =====================================================================

variable_declarators
    :   var1=variable_declarator
        (COMMA var2=variable_declarator
        )*
    ;

variable_declarator
    :   variable_declarator_id ('=' variable_initializer)?
    ;

variable_declarator_id
    :   IDENT ('[' ']')*
    ;

variable_initializer
    :   array_initializer
    |   expression
    ;

array_initializer
    :   '{' (variable_initializer (',' variable_initializer)* (',')?)? '}'
    ;

type
    :   class_or_interface_type ('[' ']')*
    |   primitive_type ('[' ']')*
    |   boxed_type ('[' ']')*
    |   limited_type ('[' ']')*
    ;

class_or_interface_type
    :   FAST_UTIL_DATA_TYPE
    ;

type_arguments
    :   '<'
        ta1=type_argument
        (COMMA ta2=type_argument
        )*
        '>'
    ;

type_argument
    :   type
    |   '?' (('extends' | 'super') type)?
    ;

formal_parameters
    :   LPAR formal_parameter_decls RPAR
    ;

formal_parameter_decls
    :   decl=formal_parameter_decl
        (COMMA decl=formal_parameter_decl
        )*
    ;
    
formal_parameter_decl
    :   variable_modifiers type variable_declarator_id
    ;

primitive_type
    :   { "boolean".equals(_input.LT(1).getText()) }? BOOLEAN
    |   'char'
    |   { "byte".equals(_input.LT(1).getText()) }? BYTE
    |   'short'
    |   { "int".equals(_input.LT(1).getText()) }? INT
    |   { "long".equals(_input.LT(1).getText()) }? LONG
    |   'float'
    |   { "double".equals(_input.LT(1).getText()) }? DOUBLE
    ;

boxed_type
    :   { "Boolean".equals(_input.LT(1).getText()) }? BOOLEAN
    |   'Character'
    |   { "Byte".equals(_input.LT(1).getText()) }? BYTE
    |   'Short'
    |   'Integer'
    |   { "Long".equals(_input.LT(1).getText()) }? LONG
    |   'Float'
    |   { "Double".equals(_input.LT(1).getText()) }? DOUBLE
    ;

limited_type
    :   'String'
    |   'System'
    |   'Math'     
    ;

variable_modifier
    :   'final'
    ;

relevance_model
    :   DEFINED AS params=formal_parameters
        BEGIN model_block END
    ;

model_block
    :   block_statement+
    ;

block
    :   '{' 
        block_statement* 
        '}'
    ;

block_statement
    :   local_variable_declaration_stmt
    |   java_statement
    ;

local_variable_declaration_stmt
    :   local_variable_declaration SEMI
    ;

local_variable_declaration
    :   variable_modifiers type variable_declarators
    ;

variable_modifiers
    :   variable_modifier*
    ;

java_statement
    :   block
    |   'if' par_expression java_statement (else_statement)?
    |   FOR LPAR
        for_control RPAR java_statement
    |   'while' par_expression java_statement
    |   'do' java_statement 'while' par_expression SEMI
    |   'switch' par_expression '{' switch_block_statement_groups '}'
    |   'return' expression SEMI
    |   'break' IDENT? SEMI
    |   'continue' IDENT? SEMI
    |   SEMI
    |   statement_expression SEMI
    ;

else_statement
    :   { "else".equals(_input.LT(1).getText()) }? ELSE java_statement
    ;

switch_block_statement_groups
    :   (switch_block_statement_group)*
    ;

switch_block_statement_group
    :   switch_label+ block_statement*
    ;

switch_label
    :   'case' constant_expression COLON
    |   'case' enum_constant_name COLON
    |   'default' COLON
    ;

for_control
    :   enhanced_for_control
    |   for_init? SEMI expression? SEMI for_update?
    ;

for_init
    :   local_variable_declaration
    |   expression_list
    ;

enhanced_for_control
    :   variable_modifiers type IDENT COLON expression
    ;

for_update
    :   expression_list
    ;

par_expression
    :   LPAR expression RPAR
    ;

expression_list
    :   expression (',' expression)*
    ;

statement_expression
    :   expression
    ;

constant_expression
    :   expression
    ;

enum_constant_name
    :   IDENT
    ;

expression
    :   conditional_expression (assignment_operator expression)?
    ;

assignment_operator
    :   '='
    |   '+='
    |   '-='
    |   '*='
    |   '/='
    |   '&='
    |   '|='
    |   '^='
    |   '%='
    |   '<' '<' '=' 
    |   '>' '>' '>' '='
    |   '>' '>' '='
    ;

conditional_expression
    :   conditional_or_expression ( '?' expression ':' expression )?
    ;

conditional_or_expression
    :   conditional_and_expression ( '||' conditional_and_expression )*
    ;

conditional_and_expression
    :   inclusive_or_expression ('&&' inclusive_or_expression )*
    ;

inclusive_or_expression
    :   exclusive_or_expression ('|' exclusive_or_expression )*
    ;

exclusive_or_expression
    :   and_expression ('^' and_expression )*
    ;

and_expression
    :   equality_expression ( '&' equality_expression )*
    ;

equality_expression
    :   instanceof_expression ( ('==' | '!=') instanceof_expression )*
    ;

instanceof_expression
    :   relational_expression ('instanceof' type)?
    ;

relational_expression
    :   shift_expression ( relational_op shift_expression )*
    ;

relational_op
    :   '<' '=' 
    |   '>' '=' 
    |   '<'
    |   '>'
    ;

shift_expression
    :   additive_expression ( shift_op additive_expression )*
    ;

shift_op
    :   '<' '<' 
    |   '>' '>' '>' 
    |   '>' '>'
    ;

additive_expression
    :   multiplicative_expression ( ('+' | '-') multiplicative_expression )*
    ;

multiplicative_expression
    :   unary_expression ( ( '*' | '/' | '%' ) unary_expression )*
    ;
    
unary_expression
    :   '+' unary_expression
    |   '-' unary_expression
    |   '++' unary_expression
    |   '--' unary_expression
    |   unary_expression_not_plus_minus
    ;

unary_expression_not_plus_minus
    :   '~' unary_expression
    |   '!' unary_expression
    |   cast_expression
    |   primary selector* ('++'|'--')?
    ;

cast_expression
    :  '(' primitive_type ')' unary_expression
    |  '(' (type | expression) ')' unary_expression_not_plus_minus
    ;

primary
    :   par_expression
    |   literal   
    |   java_method identifier_suffix
    |   java_ident ('.' java_method)* identifier_suffix?
    ;

java_ident
    :   boxed_type
    |   limited_type
    |   IDENT
    ;

// Need to handle the conflicts of BQL keywords and common Java method
// names supported by BQL.
java_method
    :   { "contains".equals(_input.LT(1).getText()) }? CONTAINS
    |   IDENT
    ;

identifier_suffix
    :   ('[' ']')+ '.' 'class'
    |   arguments
    |   '.' 'class'
    |   '.' 'this'
    |   '.' 'super' arguments
    ;

literal 
    :   integer_literal
    |   REAL
    |   FLOATING_POINT_LITERAL
    |   CHARACTER_LITERAL
    |   STRING_LITERAL
    |   boolean_literal
    |   { "null".equals(_input.LT(1).getText()) }? NULL
    ;

integer_literal
    :   HEX_LITERAL
    |   OCTAL_LITERAL
    |   INTEGER
    ;

boolean_literal
    :   { "true".equals(_input.LT(1).getText()) }? TRUE
    |   { "false".equals(_input.LT(1).getText()) }? FALSE
    ;

selector
    :   '.' IDENT arguments?
    |   '.' 'this'
    |   '[' expression ']'
    ;

arguments
    :   '(' expression_list? ')'
    ;
    
relevance_model_clause
    :   USING RELEVANCE MODEL IDENT prop_list[KeyType.STRING_LITERAL_AND_IDENT] model=relevance_model?
    ;

facet_param_list
    :   p=facet_param
        (   COMMA p=facet_param
        )*
    ;

facet_param
    :   LPAR column_name COMMA STRING_LITERAL COMMA facet_param_type COMMA (val=value | valList=non_variable_value_list) RPAR
    ;

facet_param_type
    :   t=(BOOLEAN | INT | LONG | STRING | BYTEARRAY | DOUBLE) 
    ;


fragment DIGIT : '0'..'9' ;
fragment ALPHA : 'a'..'z' | 'A'..'Z' ;

INTEGER : ('0' | '1'..'9' '0'..'9'*) INTEGER_TYPE_SUFFIX? ;
REAL : DIGIT+ '.' DIGIT* ;
LPAR : '(' ;
RPAR : ')' ;
COMMA : ',' ;
COLON : ':' ;
SEMI : ';' ;
EQUAL : '=' ;
GT : '>' ;
GTE : '>=' ;
LT : '<' ;
LTE : '<=';
NOT_EQUAL : '<>' ;
DOT : '.';
LBRACE : '{';
RBRACE : '}';
LBRACK : '[';
RBRACK : ']';
PLUS : '+';
MINUS : '-';
STAR : '*';
DIV : '/';
MOD : '%';
INC : '++';
DEC : '--';
TILDE : '~';
BANG : '!';
CARET : '^';
EQEQ : '==';
NEQ : '!=';
PLUSEQ : '+=';
MINUSEQ : '-=';
STAREQ : '*=';
DIVEQ : '/=';
AMPEQ : '&=';
PIPEEQ : '|=';
CARETEQ : '^=';
MODEQ : '%=';
OROR : '||';
ANDAND : '&&';
PIPE : '|';
AMP : '&';
QUES : '?';

STRING_LITERAL
    :   '"'
        (   '"' '"'
        |   ~('"'|'\r'|'\n')
        )*
        '"'
    |   '\''
        (   '\'' '\''
        |   ~('\''|'\r'|'\n')
        )*
        '\''
    ;

DATE
    :   DIGIT DIGIT DIGIT DIGIT ('-'|'/') DIGIT DIGIT ('-'|'/') DIGIT DIGIT 
    ;

TIME
    :
        DIGIT DIGIT ':' DIGIT DIGIT ':' DIGIT DIGIT
    ;

//
// BQL Relevance model related
//

fragment HEX_DIGIT : ('0'..'9'|'a'..'f'|'A'..'F') ;
fragment INTEGER_TYPE_SUFFIX: ('l' | 'L') ;
fragment EXPONENT : ('e'|'E') ('+'|'-')? ('0'..'9')+ ;
fragment FLOAT_TYPE_SUFFIX : ('f'|'F'|'d'|'D') ;

fragment
ESCAPE_SEQUENCE
    :   '\\' ('b'|'t'|'n'|'f'|'r'|'\"'|'\''|'\\')
    |   UNICODE_ESCAPE
    |   OCTAL_ESCAPE
    ;

fragment
UNICODE_ESCAPE
    :   '\\' 'u' HEX_DIGIT HEX_DIGIT HEX_DIGIT HEX_DIGIT
    ;

fragment
OCTAL_ESCAPE
    :   '\\' ('0'..'3') ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7') ('0'..'7')
    |   '\\' ('0'..'7')
    ;

HEX_LITERAL : '0' ('x'|'X') HEX_DIGIT+ INTEGER_TYPE_SUFFIX? ;
OCTAL_LITERAL : '0' ('0'..'7')+ INTEGER_TYPE_SUFFIX? ;

FLOATING_POINT_LITERAL
    :   REAL EXPONENT? FLOAT_TYPE_SUFFIX?
    |   '.' DIGIT+ EXPONENT? FLOAT_TYPE_SUFFIX?
    |   DIGIT+ EXPONENT FLOAT_TYPE_SUFFIX?
    |   DIGIT+ FLOAT_TYPE_SUFFIX
    ;

CHARACTER_LITERAL
    :   '\'' ( ESCAPE_SEQUENCE | ~('\''|'\\') ) '\''
    ;

//
// Java-only Keywords
//

BREAK : 'break';
CASE : 'case';
CHAR : 'char';
CHARACTER : 'Character';
CLASS : 'class';
CONTINUE : 'continue';
DEFAULT : 'default';
DO : 'do';
EXTENDS : 'extends';
FINAL : 'final';
FLOAT : 'float';
FLOAT2 : 'Float';
FOR : 'for';
IF : 'if';
INTEGER2 : 'Integer';
INSTANCEOF : 'instanceof';
MATH : 'Math';
MIN : 'min';
RETURN : 'return';
SHORT : 'short';
SHORT2 : 'Short';
STRING2 : 'String';
SUPER : 'super';
SWITCH : 'switch';
SYSTEM : 'System';
THIS : 'this';
WHILE : 'while';

//
// BQL Keywords
//

ALL : [Aa][Ll][Ll] ;
AFTER : [Aa][Ff][Tt][Ee][Rr] ;
AGAINST : [Aa][Gg][Aa][Ii][Nn][Ss][Tt] ;
AGO : [Aa][Gg][Oo] ;
AND : [Aa][Nn][Dd] ;
AS : [Aa][Ss] ;
ASC : [Aa][Ss][Cc] ;
BEFORE : [Bb][Ee][Ff][Oo][Rr][Ee] ;
BEGIN : [Bb][Ee][Gg][Ii][Nn] ;
BETWEEN : [Bb][Ee][Tt][Ww][Ee][Ee][Nn] ;
BOOLEAN : [Bb][Oo][Oo][Ll][Ee][Aa][Nn] ;
BROWSE : [Bb][Rr][Oo][Ww][Ss][Ee] ;
BY : [Bb][Yy] ;
BYTE : [Bb][Yy][Tt][Ee] ;
BYTEARRAY : [Bb][Yy][Tt][Ee][Aa][Rr][Rr][Aa][Yy] ;
CONTAINS : [Cc][Oo][Nn][Tt][Aa][Ii][Nn][Ss] ;
DEFINED : [Dd][Ee][Ff][Ii][Nn][Ee][Dd] ;
DESC : [Dd][Ee][Ss][Cc] ;
DESCRIBE : [Dd][Ee][Ss][Cc][Rr][Ii][Bb][Ee] ;
DISTINCT : [Dd][Ii][Ss][Tt][Ii][Nn][Cc][Tt] ;
DOUBLE : [Dd][Oo][Uu][Bb][Ll][Ee] ;
EMPTY : [Ee][Mm][Pp][Tt][Yy] ;
ELSE : [Ee][Ll][Ss][Ee] ;
END : [Ee][Nn][Dd] ;
EXCEPT : [Ee][Xx][Cc][Ee][Pp][Tt] ;
EXECUTE : [Ee][Xx][Ee][Cc][Uu][Tt][Ee] ;
FACET : [Ff][Aa][Cc][Ee][Tt] ;
FALSE : [Ff][Aa][Ll][Ss][Ee] ;
FETCHING : [Ff][Ee][Tt][Cc][Hh][Ii][Nn][Gg] ;
FROM : [Ff][Rr][Oo][Mm] ;
GROUP : [Gg][Rr][Oo][Uu][Pp] ;
GIVEN : [Gg][Ii][Vv][Ee][Nn] ;
HITS : [Hh][Ii][Tt][Ss] ;
IN : [Ii][Nn] ;
INT : [Ii][Nn][Tt] ;
IS : [Ii][Ss] ;
LAST : [Ll][Aa][Ss][Tt] ;
LIKE : [Ll][Ii][Kk][Ee] ;
LIMIT : [Ll][Ii][Mm][Ii][Tt] ;
LONG : [Ll][Oo][Nn][Gg] ;
MATCH : [Mm][Aa][Tt][Cc][Hh] ;
MODEL : [Mm][Oo][Dd][Ee][Ll] ;
NOT : [Nn][Oo][Tt] ;
NOW : [Nn][Oo][Ww] ;
NULL : [Nn][Uu][Ll][Ll] ;
OR : [Oo][Rr] ;
ORDER : [Oo][Rr][Dd][Ee][Rr] ;
PARAM : [Pp][Aa][Rr][Aa][Mm] ;
QUERY : [Qq][Uu][Ee][Rr][Yy] ;
ROUTE : [Rr][Oo][Uu][Tt][Ee] ;
RELEVANCE : [Rr][Ee][Ll][Ee][Vv][Aa][Nn][Cc][Ee] ;
SELECT : [Ss][Ee][Ll][Ee][Cc][Tt] ;
SINCE : [Ss][Ii][Nn][Cc][Ee] ;
STORED : [Ss][Tt][Oo][Rr][Ee][Dd] ;
STRING : [Ss][Tt][Rr][Ii][Nn][Gg] ;
TOP : [Tt][Oo][Pp] ;
TRUE : [Tt][Rr][Uu][Ee] ;
USING : [Uu][Ss][Ii][Nn][Gg] ;
VALUE : [Vv][Aa][Ll][Uu][Ee] ;
WHERE : [Ww][Hh][Ee][Rr][Ee] ;
WITH : [Ww][Ii][Tt][Hh] ;

WEEKS : [Ww][Ee][Ee][Kk][Ss]? ;
DAYS : [Dd][Aa][Yy][Ss]? ;
HOURS : [Hh][Oo][Uu][Rr][Ss]? ;
MINUTES : [Mm][Ii][Nn][Uu][Tt][Ee][Ss]? ;
MINS : [Mm][Ii][Nn][Ss]? ;
SECONDS : [Ss][Ee][Cc][Oo][Nn][Dd][Ss]? ;
SECS : [Ss][Ee][Cc][Ss]? ;
MILLISECONDS : [Mm][Ii][Ll][Ll][Ii][Ss][Ee][Cc][Oo][Nn][Dd][Ss]? ;
MSECS : [Mm][Ss][Ee][Cc][Ss]? ;

FAST_UTIL_DATA_TYPE
    :   'IntOpenHashSet'
    |   'FloatOpenHashSet'
    |   'DoubleOpenHashSet'
    |   'LongOpenHashSet'
    |   'ObjectOpenHashSet'
    |   'Int2IntOpenHashMap'
    |   'Int2FloatOpenHashMap'
    |   'Int2DoubleOpenHashMap'
    |   'Int2LongOpenHashMap'
    |   'Int2ObjectOpenHashMap'
    |   'Object2IntOpenHashMap'
    |   'Object2FloatOpenHashMap'
    |   'Object2DoubleOpenHashMap'
    |   'Object2LongOpenHashMap'
    |   'Object2ObjectOpenHashMap'
    ;

// Have to define this after the keywords?
IDENT : (ALPHA | '_') (ALPHA | DIGIT | '-' | '_')* ;
VARIABLE : '$' (ALPHA | DIGIT | '_')+ ;

WS : ( ' ' | '\t' | '\r' | '\n' )+ -> channel(HIDDEN);

COMMENT
    : '/*' .*? '*/' -> channel(HIDDEN)
    ;

LINE_COMMENT
    : '--' ~('\n'|'\r')* -> channel(HIDDEN)
    ;
