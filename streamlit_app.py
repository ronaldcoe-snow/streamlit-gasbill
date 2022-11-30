import streamlit
import pandas as pd
import snowflake.connector
from snowflake.snowpark.session import Session

pd.option_context('display.float_format', '{:0.2f}'.format)
  
def create_sp_session():
  conn_param = {
    "account": streamlit.secrets["snowflake"].account,
    "user": streamlit.secrets["snowflake"].user,
    "database": streamlit.secrets["snowflake"].database,
    "role": streamlit.secrets["snowflake"].role,
    "warehouse": streamlit.secrets["snowflake"].warehouse,
    "schema": streamlit.secrets["snowflake"].schema,
    "password": streamlit.secrets["snowflake"].password
  }
  session = Session.builder.configs(conn_param).create()
  return session

def get_demo_table_list():
  with my_cnx.cursor() as my_cur:
      my_cur.execute("SELECT * FROM DEMO_TABLE")
      return my_cur.fetchall()

def get_demo_transaction_list():
  with my_cnx.cursor() as my_cur_transactions:
      my_cur_transactions.execute("SELECT *, YEAR(transactionDate) as transactionYear, MONTH(transactionDate) as transactionMonth FROM tbl_gasbill")
      return my_cur_transactions.fetchall()

def get_demo_transaction_list_sp(the_session, t_df):
  m_df = the_session.sql("SELECT *, YEAR(transactionDate) as transactionYear, MONTH(transactionDate) as transactionMonth FROM tbl_gasbill")
  t_df = m_df.to_pandas()
  # streamlit.table(t_df)
  return t_df.copy()

def get_demo_transaction_list_w_param_year(the_year):
  with my_cnx.cursor() as my_cur_transactions:
      my_cur_transactions.execute("SELECT *, YEAR(t_date) as transactionYear, MONTH(transactionDate) as transactionMonth FROM tbl_gasbill")
      return my_cur_transactions.fetchall()


# my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
my_session = create_sp_session()


# streamlit.table(df)
r_df = pd.DataFrame()
back_from_transactions = get_demo_transaction_list_sp(my_session, r_df)
# my_cnx.close()



df_transactions = pd.DataFrame(back_from_transactions, columns=['TRANSACTIONDATE', 'TRANSACTIONAMOUNT', 'TRANSACTIONSTATUS', 'TRANSACTIONYEAR', 'TRANSACTIONMONTH'])
# streamlit.table(df_transactions)
df_transactions['TRANSACTIONDATE'] = pd.to_datetime(df_transactions['TRANSACTIONDATE'])
df_transactions['year'] = df_transactions['TRANSACTIONDATE'].dt.to_period('M')
# streamlit.table(df_transactions)
my_session.close()


df_m_rep = pd.DataFrame(df_transactions['TRANSACTIONMONTH'].unique().tolist(), columns = ['TRANSACTIONMONTH'])

df_y_rep = pd.DataFrame(df_transactions['TRANSACTIONYEAR'].unique().tolist(), columns = ['TRANSACTIONYEAR'])

filt_m = (df_transactions['TRANSACTIONMONTH'].isin(df_m_rep['TRANSACTIONMONTH'].values.tolist()))
# streamlit.write(filt_m)

filt_y = (df_transactions['TRANSACTIONYEAR'].isin(df_y_rep['TRANSACTIONYEAR'].values.tolist()))
# streamlit.write(filt_y)

df_months_represented = pd.DataFrame(df_transactions[filt_m], columns=['TRANSACTIONDATE', 'TRANSACTIONAMOUNT', 'TRANSACTIONSTATUS', 'TRANSACTIONYEAR', 'TRANSACTIONMONTH'])



df_sl_years = pd.DataFrame(df_months_represented)


t_years = [int(x) for x in df_sl_years['TRANSACTIONYEAR']]


sel_year = streamlit.slider("Select a year", min_value = int(df_sl_years['TRANSACTIONYEAR'].min()), max_value = int(df_sl_years['TRANSACTIONYEAR'].max()), value = int(df_sl_years['TRANSACTIONYEAR'].min()))


filt_slider = ((df_transactions['TRANSACTIONYEAR'] >= int(df_sl_years['TRANSACTIONYEAR'].min()) & (df_transactions['TRANSACTIONYEAR'] <= sel_year)))

f_date_str = "%Y-%m-%d"
df_transactions['cv_TRANSACTIONDATE'] = df_transactions['TRANSACTIONDATE'].dt.strftime(f_date_str)
if sel_year > 0:

  streamlit.line_chart(df_months_represented[filt_slider], x= 'TRANSACTIONMONTH', y = 'TRANSACTIONYEAR')
  # streamlit.write(df_combined_trans[['TRANSACTIONMONTH', 'Year_' + str(t_sel[0]), 'Year_' + str(t_sel[1])]].to_string(index=False))
df_months_represented[filt_slider]