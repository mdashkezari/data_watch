
import pandas as pd
from db import query




def all_tables():
    return query("select distinct Table_Name from dbo.udfCatalog()", servers=["rainier"])["Table_Name"].values



def stranded_tables(tables):
    """
    return list of table names that are mentioined in the catalog but they don't exist in database.
    """
    rainierTables = query("select * from information_schema.tables", servers=["rainier"])["TABLE_NAME"].values
    rossbyTables = query("select * from information_schema.tables", servers=["rossby"])["TABLE_NAME"].values
    marianaTables = query("select * from information_schema.tables", servers=["mariana"])["TABLE_NAME"].values

    strandedTables = []
    for t in tables:
        if (not t in rainierTables) and (not t in rossbyTables) and (not t in marianaTables):
            strandedTables.append(t)
    strandedTablesDF = pd.DataFrame({"Table": strandedTables})
    strandedTablesDF.to_csv("./export/StrandedTables.csv", index=False)
    return strandedTablesDF        


def vars_exist(table, vars, servers):
    """
    check if `vars` exist in `table` on any database server.
    doesn't check if the table is on the designated server or not (using tbldataset_servers).
    """
    found = False
    try:
        sql = f"select top 5 lat,{vars} from {table}"
        for server in servers:
            df = query(sql, servers=[server])
            if not isinstance(df, pd.DataFrame): continue
            if len(df) > 0: found = True
    except:
        return None, None
    return found, df        


def stranded_vars(servers):
    df = query("SELECT Dataset_ID, Dataset_Name, Table_Name, STRING_AGG(CONVERT(NVARCHAR(max),CONCAT('[',Variable, ']')),',' ) Variable FROM dbo.udfCatalog() GROUP BY Table_Name, Dataset_ID, Dataset_Name ORDER by Dataset_ID DESC")
    strandedVars = pd.DataFrame({})
    for index, row in df.iterrows():
        print(f"checking for stranded vars in table ({index+1}/{len(df)}): {row.Table_Name} ...")
        varsExist, _ = vars_exist(row["Table_Name"], row["Variable"], servers)
        if not varsExist:
            for v in row["Variable"].split(","):
                vExist, resp = vars_exist(row["Table_Name"], v, servers)
                if not vExist:
                    rowDF = row.to_frame().T
                    rowDF["Variable"] = v
                    rowDF["Message"] = resp
                    if len(strandedVars ) < 1:
                        strandedVars = rowDF
                    else:
                        strandedVars = pd.concat([strandedVars, rowDF], ignore_index=True)   
    strandedVars.to_csv("./export/strandedVars.csv", index=False)
    return strandedVars




def main():
    servers=["rainier", "rossby", "mariana"]
    allTables = all_tables()
    
    strandedTables = stranded_tables(allTables)
    print(strandedTables)

    strandedVars = stranded_vars(servers)
    print(strandedVars)
    return strandedTables, strandedVars


if __name__ == "__main__":
    main()






# print(query("select variable, table_name from dbo.udfCatalog() where table_name='tblArgoMerge_REP'", servers=["rainier"]))

# Table tblArgoMerge_REP not found in database


# check stranded tables
# check stranded variables
# check what index exists 

