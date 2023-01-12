from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, BatchStatement
import pandas as pd

from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File 
import io
import sys
import configparser


dir_path = r'C:\Downloads\Karteek\out'   #Output file path
bundle_path =  r'C:\Downloads'    # Astra bundles path
config = configparser.ConfigParser()
config.read('Config.ini')
sys_ks = ['datastax_sla','data_endpoint_auth','system_auth','system_schema','dse_system_local','dse_system','dse_leases','HiveMetaStore','solr_admin','dse_insights','OpsCenter','dse_insights_local','system_distributed','dse_analytics','system','dse_perf','system_traces','dse_security','dsefs','NA']


def get_DSE_session(DSEenv):
    DSEusername = config.get(DSEenv,'DSEusername')
    DSEpassword= config.get(DSEenv,'DSEpassword')
    cluster_ip_list = config.get(DSEenv,'cluster_ip_list')
    cluster_ip_port = "9042"
    auth_provider = PlainTextAuthProvider(username=DSEusername, password=DSEpassword)
    DSEcluster = Cluster([cluster_ip_list], int(cluster_ip_port),
                            connect_timeout=600.0,
                            control_connection_timeout=180.0,
                            auth_provider=auth_provider,
                            )
    return DSEcluster, DSEcluster.connect()

def get_astra_session(astraenv):
    cloud_config = {'secure_connect_bundle': '{}\{}'.format(bundle_path,config.get(astraenv,'cloud_config'))}
    cloud_config2 = {'secure_connect_bundle': '{}\{}'.format(bundle_path,config.get(astraenv,'cloud_config2'))}
    auth_provider = PlainTextAuthProvider(config.get(astraenv,'client_id'), config.get(astraenv,'client_secret'))
    Astracluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    Astrasession = Astracluster.connect()
    print('Astra-DB1 session Established')
    Astracluster2 = Cluster(cloud=cloud_config2, auth_provider=auth_provider)
    Astrasession2 = Astracluster2.connect() 
    print('Astra-DB2 session Established')
    return Astracluster, Astracluster2, Astrasession, Astrasession2

def get_common_records(DF1, DF2):
    Common_DF = pd.DataFrame(columns=['KeySpace','Table','Column','Ks.Tbl','Ks.Tbl.Col'])
    j=0
    for index,row in DF1.iterrows():
        if row['Ks.Tbl'] in DF2['Ks.Tbl'].values:
            Common_DF.loc[j] = [row['keyspace_name'],row['table_name'], row['column_name'],row['Ks.Tbl'],row['Ks.Tbl.Col']]
            j = j+1
    return Common_DF



if len(sys.argv) == 3:
    print("fetching the login credentials for: "+ sys.argv[1])
    try:
        DSEcluster, DSEsession = get_DSE_session(sys.argv[1])
        print("DSE Session Established")
    except Exception as e:
        print('Failed fetching login credentials for '+ sys.argv[1])
        print("error message: " + e)

    print("fetching the login credentials for: "+ sys.argv[2])
    try:
        Astracluster, Astracluster2, Astrasession, Astrasession2 = get_astra_session(sys.argv[2])

    except Exception as e:
        print('Failed fetching login credentials for '+ sys.argv[2])
        print("error message: " + e)

    Query = "select * from system_schema.columns"

    DSE_DF = pd.DataFrame(DSEsession.execute(Query))
    Astra_DF1 = pd.DataFrame(Astrasession.execute(Query))
    Astra_DF2 = pd.DataFrame(Astrasession2.execute(Query))

    Astra_DF = pd.concat([Astra_DF1,Astra_DF2])

    # Uncomment below code if full list of columns are needed    
    #DSE_DF.to_csv('{}\{}'.format(dir_path,'DSE_Columns.csv'))
    #Astra_DF.to_csv('{}\{}'.format(dir_path,'Astra_Columns.csv'))

    DSE_DF2 = DSE_DF[~DSE_DF.keyspace_name.isin(sys_ks)]
    #DSE_DF2.columns = ['DSE_'+str(col) for col in DSE_DF2.columns]
    Astra_DF2 = Astra_DF[~Astra_DF.keyspace_name.isin(sys_ks)]
    #Astra_DF2.columns = ['Astra_'+str(col) for col in Astra_DF2.columns]

    DSE_DF2.to_csv('{}\{}'.format(dir_path,'DSE_Columns_filtered.csv'))
    print('DSE Columns are written to file after filtering System Keyspaces')
    Astra_DF2.to_csv('{}\{}'.format(dir_path,'Astra_Columns_filtered.csv'))
    print('Astra Columns are written to file after filtering System Keyspaces')

    DSE_DF2['Ks.Tbl'] = DSE_DF2['keyspace_name'].map(str)+"."+DSE_DF2['table_name'].map(str)
    DSE_DF2['Ks.Tbl.Col'] = DSE_DF2['keyspace_name'].map(str)+"."+DSE_DF2['table_name'].map(str)+"."+DSE_DF2['column_name'].map(str)
    Astra_DF2['Ks.Tbl'] = Astra_DF2['keyspace_name'].map(str)+"."+Astra_DF2['table_name'].map(str)
    Astra_DF2['Ks.Tbl.Col'] = Astra_DF2['keyspace_name'].map(str)+"."+Astra_DF2['table_name'].map(str)+"."+Astra_DF2['column_name'].map(str)


    Dcolumns=['DSE_KeySpace','DSE_Table','DSE_Column','DSE_Ks.Tbl','DSE_Ks.Tbl.Col']   
    Acolumns=['Astra_KeySpace','Astra_Table','Astra_Column','Astra_Ks.Tbl','Astra_Ks.Tbl.Col']
    DSE_on_Astra = get_common_records(DSE_DF2,Astra_DF2)
    Astra_on_DSE = get_common_records(Astra_DF2,DSE_DF2)
    DSE_on_Astra.columns = Dcolumns
    Astra_on_DSE.columns = Acolumns

    comparison_DF = pd.merge(DSE_on_Astra[['DSE_KeySpace','DSE_Table','DSE_Column','DSE_Ks.Tbl','DSE_Ks.Tbl.Col']],\
        Astra_on_DSE[['Astra_KeySpace','Astra_Table','Astra_Column','Astra_Ks.Tbl','Astra_Ks.Tbl.Col']],\
            left_on='DSE_Ks.Tbl.Col',\
            right_on='Astra_Ks.Tbl.Col', how = 'outer')

    comparison_DF['on Astra?'] = ['No' if str(row['Astra_Ks.Tbl.Col']).lower()=='nan' else 'Yes' for index,row in comparison_DF.iterrows() ]
    comparison_DF['on DSE?'] = ['No' if str(row['DSE_Ks.Tbl.Col']).lower()=='nan' else 'Yes' for index,row in comparison_DF.iterrows() ]

    comparison_DF['DSE_Ks.Tbl.Col'] = comparison_DF['DSE_Ks.Tbl.Col'].fillna(comparison_DF['Astra_Ks.Tbl.Col'])
    comparison_DF['DSE_Ks.Tbl'] = comparison_DF['DSE_Ks.Tbl'].fillna(comparison_DF['Astra_Ks.Tbl'])
    comparison_DF['DSE_Column'] = comparison_DF['DSE_Column'].fillna(comparison_DF['Astra_Column'])

    Final_Comparison_DF = comparison_DF[['DSE_Ks.Tbl.Col','DSE_Ks.Tbl','DSE_Column','on Astra?','on DSE?']]

    Final_Comparison_DF.columns = 'Ks.Tbl.Col Ks.Tbl Column Col_exists_On_Astra? Col_exists_On_DSE?'.split()

    Final_Comparison_DF[~((Final_Comparison_DF['Col_exists_On_Astra?']=='Yes') & (Final_Comparison_DF['Col_exists_On_DSE?']=='Yes'))]\
        .to_excel('{}\{}'.format(dir_path,'ColumnsComparison_DSE_ASTRA.xlsx'),index=False)
    print('Column mismatches for tables that exist on both DB are written to file')

    tbl_comp_df = pd.DataFrame(columns=['KeySpace','Table','Ks.Tbl', 'On Astra?', 'on DSE?'])

    j=0                            
    for index,row in DSE_DF2.iterrows():
        if row['Ks.Tbl'] not in Astra_DF2['Ks.Tbl'].values:
            tbl_comp_df.loc[j] = [row['keyspace_name'],row['table_name'],row['Ks.Tbl'],'No','Yes']
            j = j+1
                            
    for index,row in Astra_DF2.iterrows():
        if row['Ks.Tbl'] not in DSE_DF2['Ks.Tbl'].values:
            tbl_comp_df.loc[j] = [row['keyspace_name'],row['table_name'],row['Ks.Tbl'],'Yes','No']
            j = j+1

    #tbl_comp_df.drop_duplicates()\
    #    .to_excel('{}\{}'.format(dir_path,'TablesComparison_DSE_ASTRA.xlsx'),index=False)


    url = r'sharepointURL'
    username = 'Username'
    password = 'password'
    ctx_auth = AuthenticationContext(url)
    if ctx_auth.acquire_token_for_user(username, password):
        ctx = ClientContext(url, ctx_auth)
        web = ctx.web
        ctx.load(web)
        ctx.execute_query()
        print("Authentication successful to Prod Tables Master file on Sharepoint")

    response = File.open_binary(ctx, url)
    # with open("./Prod Tables Master.xlsx", "wb") as local_file:
    #     local_file.write(response.content)

    bytes_file_obj = io.BytesIO()
    bytes_file_obj.write(response.content)
    bytes_file_obj.seek(0) 

    prod_mstr_raw_df = pd.read_excel(bytes_file_obj, sheet_name = 'Master List', engine='openpyxl')
    for i in range(len(prod_mstr_raw_df)):
        if prod_mstr_raw_df.iloc[i][0] == 'Date added to this list':
            break
    prod_mstr_df = prod_mstr_raw_df[i:] 
    prod_mstr_df = prod_mstr_df.rename(columns=prod_mstr_df.iloc[0].str.strip()).drop(prod_mstr_df.index[0]).reset_index(drop=True) 
    prod_mstr_df = prod_mstr_df.infer_objects() 

    prod_mstr_df = prod_mstr_df[['Keyspace.Table','Required on Astra?']]

    required_df = pd.merge(tbl_comp_df.drop_duplicates(),prod_mstr_df,\
        left_on='Ks.Tbl', right_on='Keyspace.Table', how='left')

    required_df[['KeySpace','Table','Ks.Tbl', 'On Astra?', 'on DSE?','Required on Astra?']].fillna('Not on List')\
        .to_excel('{}\{}'.format(dir_path,'TablesComparison_DSE_ASTRA.xlsx'),index=False)

    print('Mismatching tables on both DBs are written to file')

else:
    print("expecting 2 arguments for the script, but encountered " + str(len(sys.argv)-1))
    sys.exit(1)








