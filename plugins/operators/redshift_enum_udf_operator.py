from redshift.constants import REDSHIFT_CONN_ID
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from fb_required_args import require_keyword_args

class FBRedshiftEnumUDFOperator(BaseOperator):

    @apply_defaults
    @require_keyword_args(['task_id', 'table', 'dag'])
    def __init__(
        self,
        redshift_conn_id=REDSHIFT_CONN_ID,
        *args, **kwargs
    ):
        self.redshift_conn_id = redshift_conn_id
        self.table = kwargs['table']
        del kwargs['table']
        super(FBRedshiftEnumUDFOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        current_state = self.hook.get_records('SELECT * FROM {};'.format(self.table));
        
        enums_translations = {}
        
        for row in current_state:
            row = (row[0].lower(), row[1].lower(), row[2].lower(), int(row[3]), row[4].lower())
            if row[0] not in enums_translations:
                enums_translations[row[0]] = {}
            if row[1] not in enums_translations[row[0]]:
                enums_translations[row[0]][row[1]] = {}
            if row[2] not in enums_translations[row[0]][row[1]]:
                enums_translations[row[0]][row[1]][row[2]] = {}
            enums_translations[row[0]][row[1]][row[2]][row[3]] = row[4]

        udf_query = '''
        CREATE OR REPLACE FUNCTION public.enum_name_for_value_test (
            enum_name varchar, value numeric, table_name varchar, class_name varchar
        )
        returns varchar
        immutable
        as $$
            """
            This UDF translates the enum values into string equivalents for analytics.
            Function arguments:
                enum_name - the name of the enum defined in rails
                value - the value of the enum: note that this must be an int!
                table_name - the table in which this enum is defined
                class_name (optional) - The name of the class where they enum is 
                    declared. This is optional but Redshift doesn't allow for 
                    default arguments in UDFs so you have to put NULL here if you 
                    don't need to use it.

            returns:
                The translated name for the value of the enum. If we can't find
                an enum then it returns NULL.

            The enum_map file will eventully be a variable in matillion that we can
            replace daily when the process that scrapes it lands.
            """
            if value is None:
                return None
            
            # Ensure that value is an int
            if value != value.to_integral_value():
                raise TypeError
            else:
                value = int(value)
            
            enum_name = enum_name.lower()
            table_name = table_name.lower()

            enum_map = {}[table_name]

            if class_name is None:
                # look through all classes and return the first enum we find
                for source_class, enums_inside in enum_map.items():
                    if enum_name in enums_inside and value in enums_inside[enum_name]:
                        return enums_inside[enum_name][value]
            else:
                class_name = class_name.lower()
                return enum_map.get(class_name, {{}}).get(enum_name, {{}}).get(value, None)

            return None 
        $$ language plpythonu;
        '''.format(enums_translations)

        self.hook.run(udf_query, autocommit=True)
