import pandas as pd 
import numpy as np
from pathlib import Path
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

sort_col = {
    "1": "Oil Net",
    "2": "Gas Net",
    "3": "NGL Net",
    "4": "Oil Price",
    "5": "Gas  Price",
    "6": "NGL  Price",
    "8": "Oil  & Gas Rev. Net",
    "9": "Misc.  Rev. Net",
    "7": "Costs Net",
    "10": "Taxes Net",
"11": "Gas Gross",
"12": "NGL Gross",
"13": "Oil Gross",
"14": "Invest. Net",
"15": "NonDisc. CF Annual",
"16": "Cum Disc.CF"
}



formatter = logging.Formatter('%(asctime)s - %(levelname)s:%(name)s:%(message)s')

file_handler = logging.FileHandler('app.log')
file_handler.setFormatter(formatter)

logger.addHandler(file_handler)



def get_excel_files(path):
    excel_files = []
    for file in Path(path).glob('*.xls*'):
        excel_files.append(file)
        logger.info(f"Found file: {file}")
    return excel_files

def add_iso_date(file : Path):
    file_name = f"{pd.Timestamp(file.stat().st_ctime,unit='s').strftime('%Y-%m-%d')}_{file.stem}{file.suffix}"
    file.rename(file.parent.parent.joinpath('processed',file_name))
    logger.info(f"Renamed file: {file.stem} -> {file_name}")
    return file_name



def normalize_data(file : str):

    logger.info(f"Normalizing data for file: {file}")
    df = pd.read_excel(file,engine='xlrd',header=None,skiprows=388)
    #company names is always two rows before Year.
    df['company'] = df.loc[df.loc[(df[0].str.contains('Year',case=False)==True)].index - 4 ][1]
    df['company'] = df['company'].ffill().bfill()

    df1 = df.set_index((df[0].str.contains('Year',case=False)).cumsum().ffill(),append=True).copy()
    dfs = {x : frame.dropna(subset=[1,2,3,'company'],how='all').reset_index(drop=True).dropna(how='all',axis=1) for x,frame in df1.groupby(level=1)}

    cleaned_frames = {}

    # not terribly efficent, but a loop to deal with each "table" inside the excel.
    for index, dataframe in dfs.items():
        

        df_new = dataframe.copy()
        df_new = df_new.dropna(subset=[0],how='all')

        #the units and valuetypes are split from column names.
        unit_vals = df_new.iloc[0].str.split('\n',expand=True)
        units = unit_vals[unit_vals.columns[-1]].str.replace('\(|\)','',regex=True).str.strip()

        vals = unit_vals[unit_vals.columns[:-1]].fillna('').agg(' '.join,axis=1).str.strip()

        #create a MultiIndex so we can reshape the dataframe.
        df_new.columns = pd.MultiIndex.from_tuples(list(zip(vals,units)))

        #drop the first row, which is the column names.
        df_new = df_new.iloc[1:]
        df_new['Year'] = pd.to_datetime(df_new.iloc[:,0],errors='coerce').dt.strftime('%d/%m/%Y')

        #drop null rows after coercing the date values. 
        df_new = df_new.dropna(subset=df_new.iloc[:,:1].columns)

        #get the case names from the column and assign it later.
        case = df_new.iloc[:,-1].unique()[0]
        df3 = df_new.set_index('Year').stack([0,1]).unstack(0)

        # this is redundant now - but it's here for future reference.
        cols = [pd.to_datetime(x).strftime('%d/%m/%Y')[0] for x in df3.columns]
        df3 = df3.assign(caseName=case).set_index('caseName',append=True).reset_index().rename(columns={'level_1' : 'ValueType', 'level_0' : 'Units'})

        #set the column names - then set the order 
        df3.columns = ['ValueType','Units','CaseName'] + cols
        df3 = df3[['CaseName','ValueType','Units'] + cols]
        logger.info(f"Normalized data for case type: {case}")
        cleaned_frames[index] = df3

    # concat along the axis of each table - so we have one dataframe per case.
    final = pd.concat(cleaned_frames)

    k = { x : pd.to_datetime(x) for x in final.iloc[:,3:].columns}
    cols = dict(sorted(k.items(), key=lambda item: item[1])).keys()

    # sort the columns in date order 
    final = final[['CaseName','ValueType','Units'] + list(cols)]
    
    final = final.fillna(0)
    #create a categorical column to sort the value types according to spec. 
    final['ValueType'] = pd.Categorical(final['ValueType'],sort_col.values())
    final.index.names = ['idx','index_row']
    final = final.sort_values(['idx','ValueType'])

    fp = f"{pd.Timestamp('now').strftime('%Y-%m-%d')}_oildata.xlsx"
    file.parent.parent.joinpath('curated',fp)
    logger.info(f"Saving normalized data to: {file}")
    final.to_excel(file,index=False)



if __name__ == "__main__":
    logger.info("Starting normalization process")
    f = Path(__file__).parent.parent.joinpath('files/raw/unprocessed')
    files = get_excel_files(f)
    files = [add_iso_date(x) for x in files]
    for file in files:
        normalize_data(file)
    logger.info("Finished")
