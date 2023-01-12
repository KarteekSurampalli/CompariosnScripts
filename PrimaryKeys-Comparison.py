from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, BatchStatement
import pandas as pd
import numpy as np
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

def file_write(writeobj, dir, filename):
    with open('{}/{}'.format(dir,filename), 'w') as f:
        f.write(writeobj)
def excel_read(dir, filename, sheetname ):
    return pd.read_excel('{}\{}'.format(dir,filename),sheet_name= sheetname,engine='openpyxl')

def get_schema(cluster):
    return cluster.metadata.export_schema_as_string()

def get_keys_from_schema(SchemaStr):
    create_table = "create table"
    primary_key = "primary key"
    keys = ['KeySpace','TableName','PrimaryKey']
    i=0
    Schema_DF = pd.DataFrame(columns= keys)
    for query in SchemaStr.split(';'):
        ks_tbl = np.array(["NA","NA"])
        pk = "NA"
        for line in query.split('\n'):
            if create_table in line.lower():
                ks_tbl = line.lower().split(create_table, 1)[1].split()[0].split('.')
            if primary_key in line.lower():
                if line.lower().split(primary_key)[1] == ',':
                    pk = line.lower().strip().split(' ')[0]
                else:
                    pk = line.lower().split(primary_key)[1].replace('\n','')
        Schema_DF.loc[i] = [ks_tbl[0],ks_tbl[1],pk]
        i = i+1
    return Schema_DF

def get_common_records(DF1, DF2):
    Common_DF = pd.DataFrame(columns=['KeySpace','Table','Column','Ks.Tbl','Ks.Tbl.Col'])
    j=0
    for index,row in DF1.iterrows():
        if row['Ks.Tbl'] in DF2['Ks.Tbl'].values:
            Common_DF.loc[j] = [row['KeySpace'],row['TableName'], row['PrimaryKey'],row['Ks.Tbl'],row['Ks.Tbl.PK']]
            j = j+1
    return Common_DF


if len(sys.argv) == 3:
    print("fetching the login credentials for: "+ sys.argv[1])
    try:
        DSEcluster, DSEsession = get_DSE_session(sys.argv[1])
        print("DSE Session Established")
        DSchemaStr = get_schema(DSEcluster)
        print("DSE Schema Extracted as Text")
        file_write(DSchemaStr, dir_path,'DSE-Schema.txt')
        print("DSE Schema written to Text File")
    except Exception as e:
        print('Failed fetching login credentials for '+ sys.argv[1])
        print("error message: " + e)

    print("fetching the login credentials for: "+ sys.argv[2])
    try:
        Astracluster, Astracluster2, Astrasession, Astrasession2 = get_astra_session(sys.argv[2])    
        A1SchemaStr = get_schema(Astracluster)
        A2SchemaStr = get_schema(Astracluster2)
        ASchemaStr = A1SchemaStr + '\n' + A2SchemaStr
        print('Astra Schema Extracted as Text')
        file_write(ASchemaStr, dir_path, 'Astra-Schema.txt')
        print('Astra Schema written to file')

    except Exception as e:
        print('Failed fetching login credentials for '+ sys.argv[2])
        print("error message: " + e)

    writer = pd.ExcelWriter('{}\{}'.format(dir_path,'Astra-DSE-KS-Tbl-PK.xlsx'), engine = 'openpyxl')
    DSE_Schema_DF = get_keys_from_schema(DSchemaStr)   
    Astra_Schema_DF = get_keys_from_schema(ASchemaStr)    
    DSE_Schema_DF[~DSE_Schema_DF.KeySpace.isin(sys_ks)].to_excel(writer, sheet_name ='DSE')
    print('DSE KS, Tables, Keys written to file')
    Astra_Schema_DF[~Astra_Schema_DF.KeySpace.isin(sys_ks)].to_excel(writer, sheet_name ='ASTRA')  
    print('Astra KS, Tables, Keys written to file')  
    writer.close()

    DSE_DF = excel_read(dir_path,'Astra-DSE-KS-Tbl-PK.xlsx','DSE')
    Astra_DF = excel_read(dir_path,'Astra-DSE-KS-Tbl-PK.xlsx','ASTRA')

    DSE_DF['Ks.Tbl'] = DSE_DF['KeySpace']+"."+DSE_DF['TableName']
    Astra_DF['Ks.Tbl'] = Astra_DF['KeySpace']+"."+Astra_DF['TableName']
    DSE_DF['Ks.Tbl.PK'] = DSE_DF['KeySpace']+"."+DSE_DF['TableName']+"."+DSE_DF['PrimaryKey']
    Astra_DF['Ks.Tbl.PK'] = Astra_DF['KeySpace']+"."+Astra_DF['TableName']+"."+Astra_DF['PrimaryKey']

    Dcolumns = ['DSE_KeySpace','DSE_Table','DSE_Column','DSE_Ks.Tbl','DSE_Ks.Tbl.Col']
    Acolumns=['Astra_KeySpace','Astra_Table','Astra_Column','Astra_Ks.Tbl','Astra_Ks.Tbl.Col']
    DSE_on_Astra = get_common_records(DSE_DF,Astra_DF)
    Astra_on_DSE = get_common_records(Astra_DF, DSE_DF)
    DSE_on_Astra.columns = Dcolumns
    Astra_on_DSE.columns = Acolumns

    comparison_DF = pd.merge(DSE_on_Astra[['DSE_KeySpace','DSE_Table','DSE_Column','DSE_Ks.Tbl','DSE_Ks.Tbl.Col']],\
        Astra_on_DSE[['Astra_KeySpace','Astra_Table','Astra_Column','Astra_Ks.Tbl','Astra_Ks.Tbl.Col']],\
            left_on='DSE_Ks.Tbl.Col',\
            right_on='Astra_Ks.Tbl.Col', how = 'outer')

    comparison_DF['PK mismatch on Astra?'] = ['No' if str(row['Astra_Ks.Tbl.Col']).lower()=='nan' else 'Yes' for index,row in comparison_DF.iterrows() ]
    comparison_DF['PK mismatch on DSE?'] = ['No' if str(row['DSE_Ks.Tbl.Col']).lower()=='nan' else 'Yes' for index,row in comparison_DF.iterrows() ]

    comparison_DF['DSE_Ks.Tbl.Col'] = comparison_DF['DSE_Ks.Tbl.Col'].fillna(comparison_DF['Astra_Ks.Tbl.Col'])
    comparison_DF['DSE_Ks.Tbl'] = comparison_DF['DSE_Ks.Tbl'].fillna(comparison_DF['Astra_Ks.Tbl'])
    comparison_DF['DSE_Column'] = comparison_DF['DSE_Column'].fillna(comparison_DF['Astra_Column'])

    Final_Comparison_DF = comparison_DF[['DSE_Ks.Tbl.Col','DSE_Ks.Tbl','DSE_Column','PK mismatch on Astra?','PK mismatch on DSE?']]

    Final_Comparison_DF.columns = 'Ks.Tbl.PK Ks.Tbl PK PK_match_on_Astra? PK_match_on_DSE?'.split()

    Final_Comparison_DF[~((Final_Comparison_DF['PK_match_on_Astra?']=='Yes') & (Final_Comparison_DF['PK_match_on_DSE?']=='Yes'))]\
        .to_excel('{}\{}'.format(dir_path,'PrimaryKeyComparison_DSE_ASTRA.xlsx'))


else:
    print("expecting 2 arguments for the script, but encountered " + str(len(sys.argv)-1))
    sys.exit(1)

