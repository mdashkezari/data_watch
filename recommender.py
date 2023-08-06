import sys, os
import numpy as np
import pandas as pd
from db import query
import warnings
warnings.filterwarnings("ignore")




def validate_userid(user_id):
    df, _, err = query(f"select userid from tblUsers where userid={user_id}")
    if err or len(df) !=1: return False
    return True

def update_user_item():
    q = """
    with user_table as (
    select c.User_ID, rtrim(q.Table_Name) Table_Name, count(*) N_Calls from tblApi_Calls c
    join tblAPI_Query q on c.id=q.Call_ID
    where c.Query is not null and q.Table_Name like 'tbl%'
    group by c.User_ID, rtrim(q.Table_Name)
    )
    select UserID, Username, ut.Table_Name, ut.N_Calls from tblUsers u
    left join user_table ut on ut.User_ID=u.UserID
    where N_Calls > 0 and ut.Table_Name in (select distinct Table_Name from tblVariables v)
    order by u.UserID, ut.Table_Name
    """
    df, _, _ = query(q)
    cross_df = pd.crosstab(df["UserID"], df["Table_Name"])
    cross_df.to_csv("user_item.csv", index=True)   
    return get_user_item()




def get_user_item():
    fname = "user_item.csv"
    if not os.path.isfile(fname): return update_user_item()
    return pd.read_csv(fname, index_col="UserID")





def find_similar_users(ui_df, user_id):
    u_vec = np.matrix(ui_df.loc[[user_id]].values[0]).T
    # similary_score: number of common used datasets
    similar_users = (ui_df.dot(u_vec).query(f"UserID!={user_id}")
                    .sort_values(by=[0], ascending=False)
                    .rename(columns={0 : "similarity_score"}))
    return similar_users




def user_used_tables(ui_df, user_id):
    return (ui_df.loc[[user_id]].T
            .reset_index(drop=False)
            .rename(columns={"index":"Tables", user_id: "Used"})
            .query("Used==1")["Tables"].values)




def collaborative_based_filtering(ui_df, user_id):
    """
    Take ui_df dataframe (user-item matrix, here item represents a dataset table) and a user_id.
    Return a list of datasets that users similar to user_id have used but user_id has not used yet.
    """
    if not user_id in ui_df.index.values: sys.exit("invalid user_id")
    similar_users_df = find_similar_users(ui_df, user_id).query("UserID>1 and similarity_score>0")   # ignore UserID=1 (unregistered users), and users with no similarity
    # print(similar_users_df)
    uut = user_used_tables(ui_df, user_id)
    recommendations = []
    # the top user is the most simillar.
    # you may choose to only recommend based on the most similar user (as is done here), 
    # or recommend based on all users. 
    # the top recommendations are derived from the most similar users.
    for i in similar_users_df.index.values[:1]:
        if i in [1, user_id]: continue
        recommendations += [t for t in user_used_tables(ui_df, i) if t not in uut and t not in recommendations]
    return recommendations




def popular_datasets(top=-1):  
    """
    popularity: based on the percentage of users who have used the dataset
    """
    top_statement = ""
    if top != -1 and isinstance(top, int): top_statement = f"top {top}"        
    q = f"""
        select {top_statement} rtrim(Table_Name) Table_Name, count(distinct c.User_ID)/(select 0.01*count(UserID) from tblUsers) uperc from tblAPI_Query q
        join tblApi_Calls c on c.ID=q.Call_ID
        where len(rtrim(Table_Name))>1 and rtrim(Table_Name) in (select distinct Table_Name from tblVariables)
        group by Table_Name 
        order by uperc desc    
        """
    return query(q)



def recently_used_datasets(user_id, top=-1):  # popularity: based on the percentage of users who have used the dataset
    top_statement = ""
    if top != -1 and isinstance(top, int): top_statement = f"top {top}"        
    q = f"""
        select {top_statement} rtrim(Table_Name) Table_Name from 
        (
        select Table_Name, Date_Time, row_number() over (partition by table_name order by date_time desc) as rn from tblApi_Calls c
        join tblAPI_Query q on c.ID=q.Call_ID
        where c.User_ID={user_id} and Table_Name in (select distinct Table_Name from tblVariables)
        ) sq
        where sq.rn=1
        order by Date_Time desc
        """
    return query(q)





##############################################################
#                                                            #
#         Simple Dataset Recommendation Systems              #
#                                                            #
##############################################################



# user_id = 1

# if user_id == 1:    # if user is unregistered
#     pop_ds, _, _ = popular_datasets(top=10)
#     print(list(pop_ds["Table_Name"].values))
# else:
#     ############### "Use Again" Approach ###############
#     ## recently used datasets by the user him/herself  
#     recent_ds = recently_used_datasets(user_id, top=10)
#     print(list(recent_ds["Table_Name"].values))



#     ############ "Others Have Used" Approach ############
#     # recommend dataset based on other similar users
#     ui_df, _, _ = update_user_item()  # no need to keep quering the user tables, you can use the stored file `get_user_item()`
#     # ui_df = get_user_item()   
#     colab_ds = collaborative_based_filtering(ui_df, user_id)
#     print(colab_ds)





