from factor.define.growth import *
from factor.define.leverage import *
from factor.define.pricevolume import *
from factor.define.quality import *
from factor.define.size import *
from factor.define.technical import *
from factor.define.turnover import *
from factor.define.valuation import *
from factor.define.volatility import *


config = {
    'salesgq': {
        'instance': SalesGQ(),
    },
    'profitgq': {
        'instance': ProfitGQ(),
    }, 
    'ocfgq': {
        'instance': OcfGQ(),
    },
    'roegq': {
        'instance': RoeGQ(),
    },
}
