<!-- @import "../data.nosync/latex.css" -->

# 因子分析框架

## 因子定义

“巧妇难为无米之炊”，没有因子的数据，自然也无法对因子进行分析和研究。如果pandasquant添加了`provider`模块，并使用`pandasquant/tools/database.py`脚本进行数据库的构建和管理，便可以通过pandasquant中的Stock数据库接口获取数据。如果没有安装`provider`模块，则需要自己获取数据，并保存在本地，计算每一个调仓期的因子值同时进行计算。

因子定义是通过`factor/define`进行的，`factor/define/base.py`中有`FactorBase`类作为因子定义的基类，在定义不同的大类因子时，通过统计目录下的大类因子名对应的脚本实现，在每一个大类因子下必然有一个子类被命名为`Factor<Klass>`表示大类因子继承`FactorBase`，必须实现的一点是为`Factor<Klass>`添加klass属性，表示大类因子名。

如下是一个因子大类定义的例子：

```python
from factor.define import FactorBase

class FactorKlass(FactorBase):
    def __init__(self, name):
        super().__init__(name)
        self.klass = 'klass'
```

对于因子定义，需要对继承下来的因子父类中的calculate方法进行重写，只要实现calculate方法中因子的计算逻辑即可，同时需要注意的是，计算的结果需要赋值给self.factor，并且确保返回的值是一个Series。

因子类对象的factor属性是一个Series，且为双索引的，第一维是时间datetime，第二维度是股票asset，且Series的值为因子在某个时间点datetime，某个股票asset的因子值。如下为一个示例：

| date(index) | asset(index) | value | 
| :---: | :---: | :---: |
| 2020-01-01 | 000001.SZ | 0 |
|  | 000002.SZ | 0.3 |
|  | 000003.SZ | 0.2 |
|  | 000004.SZ | 0.1 |
|  | 000005.SZ | 0.4 |
| 2020-01-02 | 000001.SZ | 0|
|  | 000002.SZ | 0.3 |
|  | 000003.SZ | 0.2 |
|  | 000004.SZ | 0.1 |
|  | 000005.SZ | 0.4 |


## 因子测试

因子测试是需要因子值的支持的，如果是通过factor中define定义的因子，不需要进行什么处理，直接传入测试函数即可；如果是外部因子定义产生的结果，需要保证因子是一个Series或DataFrame，并且使用`factor.tools`中的`process_factor`装饰器对因子生成函数进行装饰。

因子测试的流程较为简单，因子值计算好后，还需要提供未来收益（计算Barra模型、IC与分层需要）与行业分类情况（为Barra回归模型需要）。框架内的未来收益可以通过`factor.tools`中的`get_forward_return`实现，参数为一个日期列表和未来收益时间段。行业分类可以通过`factor.tools`中的`get_industry_mapping`参数为一个日期列表，返回值为中信一级行业分类的每日结果。

```python
from factor.tools import get_forward_return, get_industry_mapping

forward_return = get_forward_return(['2021-01-04', '2021-01-05'], period=1)
industry = get_industry_mapping(['2021-01-04', '2021-01-05'])
```

因子测试需要使用`factor.tools`中`factor_analysis`对因子进行分析，`factor_analysis`函数原型如下：

```python
def factor_analysis(factor: pd.Series, forward_return: pd.Series, grouper: pd.Series,
    benchmark: pd.Series = None, ic_grouped: bool = True, q: int = 5, 
    commission: float = 0.001, commision_type: str = 'both',
    datapath: str = None, show: bool = True, imagepath: str = None, 
    savedata: list = ['reg', 'ic', 'layering', 'turnover']):
    '''Factor test pipeline
    ----------------------

    factor: pd.Series, factor to test
    forward_return: pd.Series, forward return of factor
    grouper: pd.Series, grouper of factor
    benchmark: pd.Series, benchmark of factor
    ic_grouped: bool, whether to calculate IC in certain groups
    q: int, q-quantile
    commission: float, commission rate
    commission_type: str, commission type, 'both', 'buy', 'sell'
    datapath: str, path to save result, must be excel file
    show: bool, whether to show result
    imagepath: str, path to save image, must be png file
    savedata: list, data to save, ['reg', 'ic', 'layering', 'turnover']
    '''
```
