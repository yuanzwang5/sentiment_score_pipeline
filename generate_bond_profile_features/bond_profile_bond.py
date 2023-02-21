import pandas as pd
import numpy as np
import datetime as dt


def get_bond_type(cbonddescription, cbondindustrywind):
    """
    :param cbonddescription:
    :param cbondindustrywind:
    :return: bond type dataframe after filtering
    """
    bond_type = cbonddescription[['s_info_windcode', 'b_info_specialbondtype']].merge(
        cbondindustrywind[['s_info_windcode', 's_info_industryname', 's_info_industryname2']],
        how='left', on=['s_info_windcode'])
    bond_type['type1'] = np.select([bond_type.s_info_industryname == '同业存单',
                                    bond_type.s_info_industryname == '地方政府债',
                                    bond_type.s_info_industryname == '短期融资券',
                                    bond_type.s_info_industryname == '资产支持证券',
                                    bond_type.s_info_industryname == '中期票据',
                                    bond_type.s_info_industryname == '公司债',
                                    bond_type.s_info_industryname == '企业债',
                                    bond_type.s_info_industryname == '定向工具',
                                    bond_type.s_info_industryname == '金融债',
                                    bond_type.s_info_industryname == '国债',
                                    bond_type.s_info_industryname == '央行票据',
                                    bond_type.s_info_industryname == '可转债',
                                    bond_type.s_info_industryname == '可交换债',
                                    bond_type.s_info_industryname == '政府支持机构债',
                                    bond_type.s_info_industryname == '标准化票据',
                                    bond_type.s_info_industryname == '项目收益票据',
                                    bond_type.s_info_industryname == '国际机构债',
                                    bond_type.s_info_industryname == '可分离转债存债'],
                                   ['Certificates of Deposit-Interbank',
                                    'Municipal Bond',
                                    'Commercial Paper',
                                    'ABS/ABN',
                                    'Medium Term Note',
                                    'Corporate Bond',
                                    'Enterprise Bond',
                                    'PPN',
                                    'Financial Bond',
                                    'Treasury',
                                    'Central Bank Bill',
                                    'Convertible Bond',
                                    'Convertible Bond',
                                    'Government Supporting Agency Bond',
                                    'Standard Note',
                                    'Project Revenue Note',
                                    'Global Institution Bond',
                                    'Convertible Bond-Detachable'
                                    ],
                                   default=None)
    bond_type['type2'] = np.select([bond_type.s_info_industryname2 == '政策银行债',
                                    bond_type.s_info_industryname2 == '私募债',
                                    bond_type.s_info_industryname2 == '超短期融资债券',
                                    bond_type.s_info_industryname2 == '证监会主管ABS',
                                    bond_type.s_info_industryname2 == '银保监会主管ABS',
                                    bond_type.s_info_industryname2 == '交易商协会ABN',
                                    bond_type.s_info_industryname2 == '商业银行债',
                                    bond_type.s_info_industryname2 == '商业银行次级债券',
                                    bond_type.s_info_industryname2 == '证券公司债',
                                    bond_type.s_info_industryname2 == '证券公司短期融资券',
                                    bond_type.s_info_industryname2 == '保险公司债',
                                    bond_type.s_info_industryname2 == '其它金融机构债',
                                    bond_type.s_info_industryname2 == '集合票据',
                                    bond_type.s_info_industryname2 == '集合企业债'],
                                   ['Policy Bank',
                                    'Private Placement',
                                    'Super Short',
                                    'CRSC',
                                    'CBIRC',
                                    'NAFMII',
                                    'Commercial Bank',
                                    'Commercial Bank Subprime',
                                    'Securities Firm',
                                    'Securities Firm Commerical Paper',
                                    'Insurance Company',
                                    'Other Institutions',
                                    'Collective',
                                    'Collective'],
                                   default=None)
    bond_type['type3'] = np.select([bond_type.b_info_specialbondtype == '中期票据',
                                    bond_type.b_info_specialbondtype == '公司债',
                                    bond_type.b_info_specialbondtype == '可分离转债',
                                    bond_type.b_info_specialbondtype == '可转债',
                                    bond_type.b_info_specialbondtype == '同业存单',
                                    bond_type.b_info_specialbondtype == '地方政府债',
                                    bond_type.b_info_specialbondtype == '大额存单',
                                    bond_type.b_info_specialbondtype == '央行票据',
                                    bond_type.b_info_specialbondtype == '短期融资券',
                                    ],
                                   ['Medium Term Note',
                                    'Corporate Bond',
                                    'Convertible Bond-Detachable',
                                    'Convertible Bond',
                                    'Certificates of Deposit-Interbank',
                                    'Municipal Bond',
                                    'Certificates of Deposit',
                                    'Central Bank Bill',
                                    'Commercial Paper'
                                    ], default=None)
    bond_type['bond_type'] = bond_type['type1'] + np.where(bond_type['type2'].isnull(),
                                                           str(''), str('-') + bond_type['type2'])
    bond_type['bond_type'] = np.where(bond_type.bond_type.isnull(), bond_type['type3'], bond_type.bond_type)

    bond_type['bond_type'] = np.where((bond_type.bond_type == 'PPN') & (bond_type.type3 == 'Medium Term Note'),
                                      'Medium Term Note-PPN',
                                      np.where((bond_type.bond_type == 'PPN') & (bond_type.type3 == 'Commercial Paper'),
                                               'Commercial Paper-PPN', bond_type.bond_type))
    bond_type['bond_type'] = bond_type['bond_type'].replace({
        'Financial Bond-Securities Firm Commerical Paper': 'Commercial Paper-Securities Firm'
    })

    return bond_type[['s_info_windcode', 'bond_type']]


def bond_filter(bond):
    """
    :param bond: bond type dataframe
    :return: bond type and bond group dataframe after filtering
    """

    bond.loc[(bond['bond_type'].isin({'Commercial Paper-Super Short',
                                      'Commercial Paper',
                                      'Commercial Paper-Securities Firm',
                                      'Commercial Paper-PPN'})), 'bond_group'] = 'Commercial Paper'

    bond.loc[(bond['bond_type'].isin({'Medium Term Note',
                                      'Medium Term Note-PPN'})), 'bond_group'] = 'Medium Term Note'

    bond.loc[(bond['bond_type'].isin({'Corporate Bond',
                                      'Corporate Bond-Private Placement'})), 'bond_group'] = 'Corporate Bond'

    bond.loc[(bond['bond_type'].isin({'Enterprise Bond'})), 'bond_group'] = 'Enterprise Bond'

    bond.loc[(bond['bond_type'].isin({'Treasury',
                                      'Central Bank Bill'})), 'bond_group'] = 'Treasury'

    bond.loc[(bond['bond_type'].isin({'Financial Bond-Policy Bank'})), 'bond_group'] = 'Policy Bank Bond'

    bond.loc[(bond['bond_type'].isin({'Financial Bond-Securities Firm',
                                      'Financial Bond-Commercial Bank Subprime',
                                      'Financial Bond-Commercial Bank',
                                      'Financial Bond-Other Institutions',
                                      'Financial Bond-Insurance Company'})), 'bond_group'] = 'Financial Bond'

    bond.loc[(bond['bond_type'].isin({'Municipal Bond'})), 'bond_group'] = 'Municipal Bond'

    bond.loc[(bond['bond_type'].isin({'Convertible Bond',
                                      'Convertible Bond-Detachable'})), 'bond_group'] = 'Convertible Bond'

    bond['bond_group'] = np.where(bond['bond_group'].isnull(), 'Other', bond['bond_group'])

    # Bond filtering
    filter_df = (~bond['bond_group'].isin(['Other']))
    bond = bond[filter_df]

    return bond
