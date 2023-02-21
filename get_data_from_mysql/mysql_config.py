class MySQL_config():
    def __init__(self, data_date=None):
        self.save_home_dir = '/mnt/utnfs/data/sentiment_score_pipeline/data/raw_data/updated_data/cbond_tables'
        self.host = '192.168.2.81'
        self.port = '3306'
        self.user = 'intern'
        self.pw = 'dksSA648dsD#d'
        self.mode = 'prod'
        # self.table_list = ['CBONDPRICESNET', 'COMPINTRODUCTION', 'CBONDCF', 'CBONDACTUALCF',
        #                    'CBONDDESCRIPTIONZL', 'CBONDISSUER', 'CBONDDESCRIPTIONZL',
        #                    'CBONDINDUSTRYWINDZL', 'CBONDGUARANTEEINFOZL', 'CBONDSPECIALCONDITIONSZL', 'CBONDAMOUNT',
        #                     'CBONDFUNDUSING', 'CBONDINSIDEHOLDER', 'CBONDBALANCESHEET',
        #                    'CBONDINCOME', 'CBONDCASHFLOW', 'CBONDDEFAULTREPORTFORM', 'CBONDISSUERRATING', 'CBONDRATING',
        #                    'CBONDRATINGDEFINITION', 'CREDITRATINGDESCRIPTIONZL', 'CBONDCALENDAR', 'COMPANYLINEOFCREDIT'
        #                    ]
        # self.table_list = ['CBONDPRICESNET', 'COMPINTRODUCTION', 'CBONDCF', 'CBONDACTUALCF', 'CBONDDESCRIPTIONZL',
        #                    'CBONDISSUERZL', 'CBONDDESCRIPTIONZL', 'CBONDINDUSTRYWINDZL', 'CBONDGUARANTEEINFOZL',
        #                    'CBONDSPECIALCONDITIONSZL', 'CBONDAMOUNT', 'CBONDFUNDUSING', 'CBONDINSIDEHOLDER',
        #                    'CBONDBALANCESHEET', 'CBONDINCOME', 'CBONDCASHFLOW', 'CBONDDEFAULTREPORTFORM',
        #                    'CBONDISSUERRATING', 'CBONDRATING', 'CBONDRATINGDEFINITION', 'CREDITRATINGDESCRIPTIONZL',
        #                    'CBONDCALENDARZL', 'COMPANYLINEOFCREDIT', 'FINANCIALNEWS'
        #                    ]
        self.table_list = ['CBONDDESCRIPTIONZL', 'CBONDISSUERZL', 'CBONDINDUSTRYWINDZL', 'CBONDDEFAULTREPORTFORM', 'FINANCIALNEWS']

if __name__ == '__main__':
    config = MySQL_config()
