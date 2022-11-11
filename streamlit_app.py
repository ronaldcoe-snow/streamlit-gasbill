import streamlit
import pandas as pd
import snowflake.connector

pd.option_context('display.float_format', '{:0.2f}'.format)
  
def get_demo_table_list():
  with my_cnx.cursor() as my_cur:
      my_cur.execute("SELECT * FROM DEMO_TABLE")
      return my_cur.fetchall()

def get_demo_transaction_list():
  with my_cnx.cursor() as my_cur_transactions:
      my_cur_transactions.execute("SELECT *, YEAR(transactionDate) as transactionYear, MONTH(transactionDate) as transactionMonth FROM tbl_gasbill")
      return my_cur_transactions.fetchall()

def get_demo_transaction_list_w_param_year(the_year):
  with my_cnx.cursor() as my_cur_transactions:
      my_cur_transactions.execute("SELECT *, YEAR(t_date) as transactionYear, MONTH(transactionDate) as transactionMonth FROM tbl_gasbill")
      return my_cur_transactions.fetchall()


my_cnx = snowflake.connector.connect(**streamlit.secrets["snowflake"])
# back_from_function = get_demo_table_list()
# my_cnx.close()

# df = pd.DataFrame(back_from_function, columns=['First', 'Last', 'Age'])
# streamlit.dataframe(df)


# streamlit.table(df)

back_from_transactions = get_demo_transaction_list()
my_cnx.close()



df_transactions = pd.DataFrame(back_from_transactions, columns=['transactionDate', 'transactionAmount', 'transactionStatus', 'transactionYear', 'transactionMonth'])
df_transactions['year'] = df_transactions['transactionDate'].dt.to_period('M')
# streamlit.table(df_transactions)


df_m_rep = pd.DataFrame(df_transactions['transactionMonth'].unique().tolist(), columns = ['transactionMonth'])

df_y_rep = pd.DataFrame(df_transactions['transactionYear'].unique().tolist(), columns = ['transactionYear'])

filt_m = (df_transactions['transactionMonth'].isin(df_m_rep['transactionMonth'].values.tolist()))
# streamlit.write(filt_m)

filt_y = (df_transactions['transactionYear'].isin(df_y_rep['transactionYear'].values.tolist()))
# streamlit.write(filt_y)

df_months_represented = pd.DataFrame(df_transactions[filt_m], columns=['transactionDate', 'transactionAmount', 'transactionStatus', 'transactionYear', 'transactionMonth'])

# streamlit.table(df_months_represented.drop_duplicates(subset='transactionMonth'))

# streamlit.table(df_months_represented.drop_duplicates(subset='transactionYear'))

df_sl_years = df_months_represented['transactionYear'].drop_duplicates()
df_sl_years_0 = df_sl_years.to_frame().reset_index()
df_sl_years_0 = df_sl_years_0.rename(columns={0: 'transactionYear'})
# df_sl_years_0.set_index(['transactionYear'], inplace=True)

# streamlit.table(df_sl_years_0)

t_years = df_sl_years_0['transactionYear'].values.tolist()

# streamlit.write(t_years)

# my_option = streamlit.selectbox("The Year: ", df_sl_years)

# filt_one = (df_transactions['transactionYear'] == my_option)
# streamlit.table(df_transactions[filt_one])

# streamlit.write(t_years)

# streamlit.write(t_years)

streamlit.title("Compare expenses associated between two years of natural gas bills:")
t_sel = streamlit.multiselect("What Years to compare?", df_sl_years, max_selections=2)

# streamlit.write(t_sel)

# streamlit.write(len(t_sel))

f_date_str = "%Y-%m-%d"
df_transactions['cv_transactionDate'] = df_transactions['transactionDate'].dt.strftime(f_date_str)
if len(t_sel) == 2:
  c1, c2 = streamlit.columns(2)

  c1.subheader("Year One: " + str(t_sel[0]))
  c2.subheader("Year Two: " + str(t_sel[1]))
  with c1:
    filt_c1 = (df_transactions['transactionYear'] == t_sel[0])
    df_transactions_f1 = df_transactions[filt_c1].sort_values(by='transactionDate', ascending=False)
    streamlit.table(df_transactions_f1[['cv_transactionDate', 'transactionAmount']])
    df_transactions_f1.rename(columns={'transactionAmount': 'transactionAmount_1'}, inplace=True)

  with c2:
    filt_c2 = (df_transactions['transactionYear'] == t_sel[1])
    df_transactions_f2 = df_transactions[filt_c2].sort_values(by='transactionDate', ascending=False)
    streamlit.table(df_transactions_f2[['cv_transactionDate', 'transactionAmount']])
    df_transactions_f2.rename(columns={'transactionAmount': 'transactionAmount_2'}, inplace=True)
 
  t_year1 = 'Year_' + str(t_sel[0])
  t_year2 = 'Year_' + str(t_sel[1])
 
  df_combined_trans = df_transactions_f1.merge(df_transactions_f2, how="inner", on='transactionMonth')
  df_combined_trans.rename(columns={'transactionAmount_1': t_year1, 'transactionAmount_2': t_year2}, inplace=True)
  # df_combined_trans = pd.DataFrame()
  # df_combined_trans['transactionMonth'] = df_transactions_f1['transactionMonth']
  # df_combined_trans['transactionMonth'] = df_transactions_f2['transactionMonth']
  # df_combined_trans['transactionAmount_1'] = df_transactions_f1['transactionAmount']
  # df_combined_trans['transactionAmount_2'] = df_transactions_f2['transactionAmount']
  streamlit.title("Merged DataFrame of the selected years:")
  # streamlit.table(df_combined_trans[['transactionMonth', 'Year_' + str(t_sel[0]), 'Year_' + str(t_sel[1])]].to_string(index=False))
  streamlit.table(df_combined_trans[['transactionMonth', 'Year_' + str(t_sel[0]), 'Year_' + str(t_sel[1])]])

  streamlit.area_chart(df_combined_trans, x= 'transactionMonth', y = [t_year1, t_year2])
  streamlit.write(df_combined_trans[['transactionMonth', 'Year_' + str(t_sel[0]), 'Year_' + str(t_sel[1])]].to_string(index=False))
