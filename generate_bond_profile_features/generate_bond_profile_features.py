import sys
sys.path.insert(0, '/mnt/utnfs/data/sentiment_score_pipeline/python_code/v0.1.0/generate_bond_profile_features')
import pandas as pd
import bond_profile_processing
import bond_profile_bond
from bond_profile_config import Bond_profile_config
import os
import warnings

warnings.filterwarnings("ignore")


def generate_bond_profile_features(data_date=None):
    bond_profile_config = Bond_profile_config(data_date=data_date)
    # ----- Load Raw Data ----- #
    ####################################################################################################################
    input_path = bond_profile_config.cbond_tables_dir
    output_path = bond_profile_config.bond_profile_output_dir

    cbonddescription = pd.read_pickle(os.path.join(input_path, 'cbonddescriptionzl.pkl.gz'))
    cbondissuer = pd.read_pickle(os.path.join(input_path, 'cbondissuerzl.pkl.gz'))
    cbondindustrywind = pd.read_pickle(os.path.join(input_path, 'cbondindustrywindzl.pkl.gz'))
    cbonddefaultreportform = pd.read_pickle(os.path.join(input_path, 'cbonddefaultreportform.pkl.gz'))

    # ----- Pre-processing ----- #
    ####################################################################################################################
    cbonddefaultreportform = bond_profile_processing.cbonddefaultreportform_preprocess(cbonddefaultreportform)

    # ----- Combine bond information ----- #
    ####################################################################################################################
    # Generate bond type and bond group information
    df_bond_type = bond_profile_bond.get_bond_type(cbonddescription, cbondindustrywind)
    df_bond_type = bond_profile_bond.bond_filter(df_bond_type).reset_index(drop=True)
    
    # Merge bond information dataframes together to get bond profile
    df_bond = cbondissuer[['s_info_windcode', 's_info_compcode']] \
        .merge(cbonddescription[['s_info_windcode', 'b_info_issuercode', 'b_info_fullname', 'b_info_issuer', 'b_info_maturitydate']], 
               on=['s_info_windcode'], how='outer').merge(cbonddefaultreportform, on=['s_info_windcode'], how='left')
    # df_bond = df_bond.groupby('s_info_windcode').apply(lambda x: x.fillna(method='ffill').fillna(method='bfill'))
    
    # Merge bond profile with bond type dataframe together and filter out those 4 groups bond and issuers we use finally
    df_bond = df_bond.merge(df_bond_type, on=['s_info_windcode'], how='left')
    df_bond = df_bond[df_bond.bond_group.isin(['Enterprise Bond', 'Corporate Bond', 'Commercial Paper', 'Medium Term Note'])].reset_index(drop=True)

    print('[x] df_bond Done')

    # ----- Export ----- #
    ####################################################################################################################
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    df_bond.to_pickle(os.path.join(output_path, bond_profile_config.bond_profile))

    return df_bond


if __name__ == '__main__':
    generate_bond_profile_features()
