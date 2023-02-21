import numpy as np
import pandas as pd

def cbonddefaultreportform_preprocess(cbonddefaultreportform):
    """
    :param cbonddefaultreportform:
    :return: filtered cbonddefaultreportform table:
    """
    cbonddefaultreportform = cbonddefaultreportform[['b_info_windcode','b_default_date']]. \
        sort_values(['b_info_windcode','b_default_date'], ascending=(True, True)).reset_index(drop=True)
    
    # Get each b_info_windcode's first default record
    cbonddefaultreportform = cbonddefaultreportform.drop_duplicates(['b_info_windcode'], keep='first').reset_index(drop=True)
    cbonddefaultreportform.columns = ['s_info_windcode', 'b_default_date']
    
    return cbonddefaultreportform

def cbonddescription_preprocess(cbonddescription):
    """
    :param cbonddescription:
    :return: filtered cbonddescription table
    """
    filter = (cbonddescription.is_failure == 0) & (cbonddescription.b_info_par == 100) \
             & (cbonddescription.crncy_code == 'CNY') & (cbonddescription.b_info_issueprice <= 200)
    cbonddescription = cbonddescription[filter].drop_duplicates(['s_info_windcode'])

    cbonddescription['issuer_type'] = np.select([cbonddescription.b_info_issuertype == '财政部',
                                                 cbonddescription.b_info_issuertype == '中国人民银行',
                                                 cbonddescription.b_info_issuertype == '地方政府',
                                                 cbonddescription.b_info_issuertype == '政策性银行',
                                                 cbonddescription.b_info_issuertype == '国有商业银行',
                                                 cbonddescription.b_info_issuertype == '股份制商业银行',
                                                 cbonddescription.b_info_issuertype == '城市商业银行',
                                                 cbonddescription.b_info_issuertype == '证券公司',
                                                 cbonddescription.b_info_issuertype == '其它金融机构',
                                                 cbonddescription.b_info_issuertype == '企业',
                                                 cbonddescription.b_info_issuertype == '国际机构'],
                                                ['Ministry of Finance',
                                                 'Central Bank',
                                                 'Local Government',
                                                 'Policy Bank',
                                                 'SOE Commercial Bank',
                                                 'Commerical Bank',
                                                 'City Commerical Bank',
                                                 'Securities Firm',
                                                 'Other Financial Institution',
                                                 'Enterprise',
                                                 'Overseas Institution'
                                                 ],
                                                default=None)

    return cbonddescription


def cbondissuer_preprocess(cbondissuer):
    """
    :param cbondissuer:
    :return: cbondissuer with translated columns

    description:
        Translate b_agency_guarantornature, s_info_compind_name1, s_info_compind_name2, s_info_comptype to
        English and rename to ['issuer_nature' ,'industry_level1', 'industry_level2', 'comp_type']
    """
    cbondissuer['issuer_nature'] = np.select([cbondissuer.b_agency_guarantornature == '中央国有企业',
                                              cbondissuer.b_agency_guarantornature == '地方国有企业',
                                              cbondissuer.b_agency_guarantornature == '国有企业',
                                              cbondissuer.b_agency_guarantornature == '中外合资企业',
                                              cbondissuer.b_agency_guarantornature == '外商独资企业',
                                              cbondissuer.b_agency_guarantornature == '外资企业',
                                              cbondissuer.b_agency_guarantornature == '公众企业',
                                              cbondissuer.b_agency_guarantornature == '集体企业',
                                              cbondissuer.b_agency_guarantornature == '民营企业',
                                              cbondissuer.b_agency_guarantornature == '其他企业'],
                                             ['Central State-owned Enterprise',
                                              'Local State-owned Enterprise',
                                              'State-owned Enterprise',
                                              'Sino-foreign Joint Venture',
                                              'Exclusively Foreign-owned Enterprise',
                                              'Foreign-owned Enterprise',
                                              'Public Enterprise',
                                              'Collective Enterprise',
                                              'Private Enterprise',
                                              None],
                                             default=None)
    cbondissuer['industry_level1'] = np.select([cbondissuer.s_info_compind_name1 == '信息技术',
                                                cbondissuer.s_info_compind_name1 == '公用事业',
                                                cbondissuer.s_info_compind_name1 == '可选消费',
                                                cbondissuer.s_info_compind_name1 == '医疗保健',
                                                cbondissuer.s_info_compind_name1 == '工业',
                                                cbondissuer.s_info_compind_name1 == '房地产',
                                                cbondissuer.s_info_compind_name1 == '日常消费',
                                                cbondissuer.s_info_compind_name1 == '材料',
                                                cbondissuer.s_info_compind_name1 == '电信服务',
                                                cbondissuer.s_info_compind_name1 == '能源',
                                                cbondissuer.s_info_compind_name1 == '金融'],
                                               ['Information Technology', 'Utilities', 'Health Care',
                                                'Consumer Discretionary', 'Industrials', 'Real Estate',
                                                'Consumer Staples', 'Materials', 'Telecommunication Services',
                                                'Energy', 'Financials'], default=None)

    cbondissuer['industry_level2'] = np.select([cbondissuer.s_info_compind_name2 == '资本货物',
                                                cbondissuer.s_info_compind_name2 == '媒体Ⅱ',
                                                cbondissuer.s_info_compind_name2 == '运输',
                                                cbondissuer.s_info_compind_name2 == '多元金融',
                                                cbondissuer.s_info_compind_name2 == '食品、饮料与烟草',
                                                cbondissuer.s_info_compind_name2 == '材料Ⅱ',
                                                cbondissuer.s_info_compind_name2 == '商业和专业服务',
                                                cbondissuer.s_info_compind_name2 == '软件与服务',
                                                cbondissuer.s_info_compind_name2 == '耐用消费品与服装',
                                                cbondissuer.s_info_compind_name2 == '公用事业Ⅱ',
                                                cbondissuer.s_info_compind_name2 == '技术硬件与设备',
                                                cbondissuer.s_info_compind_name2 == '房地产Ⅱ',
                                                cbondissuer.s_info_compind_name2 == '制药、生物科技与生命科学',
                                                cbondissuer.s_info_compind_name2 == '半导体与半导体生产设备',
                                                cbondissuer.s_info_compind_name2 == '能源Ⅱ',
                                                cbondissuer.s_info_compind_name2 == '消费者服务Ⅱ',
                                                cbondissuer.s_info_compind_name2 == '汽车与汽车零部件',
                                                cbondissuer.s_info_compind_name2 == '保险Ⅱ',
                                                cbondissuer.s_info_compind_name2 == '零售业',
                                                cbondissuer.s_info_compind_name2 == '医疗保健设备与服务',
                                                cbondissuer.s_info_compind_name2 == '家庭与个人用品',
                                                cbondissuer.s_info_compind_name2 == '银行',
                                                cbondissuer.s_info_compind_name2 == '食品与主要用品零售Ⅱ',
                                                cbondissuer.s_info_compind_name2 == '电信服务Ⅱ'],
                                               ['Capital Goods',
                                                'Media',
                                                'Transportation',
                                                'Diversified Financials',
                                                'Food, Beverage & Tobacco',
                                                'Materials',
                                                'Commercial Services & Supplies',
                                                'Software & Services',
                                                'Consumer Durables & Apparel',
                                                'Utilies',
                                                'Techonology Hardware & Equipment',
                                                'Real Estate', 'Pharmaceuticals & Biotechnology',
                                                'Semiconductors & Semiconductor Equipment',
                                                'Energy',
                                                'Consumer Services',
                                                'Automobiles & Components', 'Insurance',
                                                'Retailing',
                                                'Health Care Equipment & Services',
                                                'Household & Personal Products',
                                                'Banks',
                                                'Food & Staples Retailing',
                                                'Telecommunication Services'], default=None)

    cbondissuer['comp_type'] = np.select([cbondissuer.s_info_comptype == '商业银行',
                                          cbondissuer.s_info_comptype == '其他公司',
                                          cbondissuer.s_info_comptype == '政策性银行',
                                          cbondissuer.s_info_comptype == '中央部委',
                                          cbondissuer.s_info_comptype == '金融租赁公司',
                                          cbondissuer.s_info_comptype == '国资委直属企业',
                                          cbondissuer.s_info_comptype == '其他金融机构',
                                          cbondissuer.s_info_comptype == '担保公司',
                                          cbondissuer.s_info_comptype == '信用社',
                                          cbondissuer.s_info_comptype == '信托公司',
                                          cbondissuer.s_info_comptype == '机关事业单位',
                                          cbondissuer.s_info_comptype == '金融资产管理公司',
                                          cbondissuer.s_info_comptype == '投资咨询公司',
                                          cbondissuer.s_info_comptype == '香港主板上市公司',
                                          cbondissuer.s_info_comptype == '国有资产管理机构',
                                          cbondissuer.s_info_comptype == '保险公司(财险)',
                                          cbondissuer.s_info_comptype == '保险公司(寿险)',
                                          cbondissuer.s_info_comptype == '财务公司',
                                          cbondissuer.s_info_comptype == '停业公司',
                                          cbondissuer.s_info_comptype == '金融控股集团',
                                          cbondissuer.s_info_comptype == '保险公司',
                                          cbondissuer.s_info_comptype == '期货公司',
                                          cbondissuer.s_info_comptype == '证券公司专项产品',
                                          cbondissuer.s_info_comptype == 'QFII机构',
                                          cbondissuer.s_info_comptype == '企业集团',
                                          cbondissuer.s_info_comptype == '外资金融机构',
                                          cbondissuer.s_info_comptype == '会计师事务所',
                                          cbondissuer.s_info_comptype == '保险公司(养老险)',
                                          cbondissuer.s_info_comptype == '保险公司(健康险)',
                                          cbondissuer.s_info_comptype == '期货经纪公司',
                                          cbondissuer.s_info_comptype == '农村商业银行'],
                                         ['Commercial Bank',
                                          None,
                                          'Policy Bank',
                                          'Central Ministry',
                                          'Financial Leasing Company',
                                          'SASAC-owned Enterprise',
                                          'Other Financial Institution',
                                          'Financing Guarantee Company',
                                          'Credit Union',
                                          'Trust Company',
                                          'State Organs and Public Institution',
                                          'Financial Asset Management Company',
                                          'Investment Consulting Company',
                                          'HK-listed Company',
                                          'State-owned Asset Management Institution',
                                          'Insurance Company (Finance)',
                                          'Insurance Company (Life)',
                                          'Finance Company',
                                          'Defunct Company',
                                          'Financal Conglomerate',
                                          'Insurance Company',
                                          'Futures Company',
                                          'Securities Company Special Product',
                                          'QFII',
                                          'Enterprise Conglomerate',
                                          'Foreign Financial Institution',
                                          'Accounting Firm',
                                          'Insurance Company (Pension)',
                                          'Insurance Company (Health)',
                                          'Futures Broker',
                                          'Rural Commercial Bank'], default=None)
    # drop columns
    cbondissuer = cbondissuer.drop(columns=['b_agency_guarantornature', 's_info_compind_name1',
                                            's_info_compind_name2', 's_info_comptype'])
    return cbondissuer


