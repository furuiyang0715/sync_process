### 指数月更新逻辑 

    (1) 给出需要进行更新的指数列表： 
    
    lst = [
    "000001",  # 上证指数
    "399001",  # 深证成指
    ... 
    ]
    
    (2) 根据 const_secumainall 找到对应的 inner_code
    
    select SeCucode, InnerCode from const_secumainall where SecuCategory=4 AND SecuCode in ("000001", "399001");
    
    map = {
        1: 000001, 
        1055: 399001, 
        ... 
    }
    
    
    (3) 根据 inner_code 进行成分股查询 查询出的 SecuCode(即成分股) 是后缀模式 
    
    select SecuCode, Weight from index_indexcomponentsweight where IndexCode = 1
    and EndDate = (SELECT max(EndDate) FROM index_indexcomponentsweight where IndexCode = 1);
    
    |         1 | 601225.XSHG | 0.259000 |
    |         1 | 601555.XSHG | 0.086000 |
    |         1 | 603001.XSHG | 0.010000 |
    |         1 | 601800.XSHG | 0.310000 |
    
    
    (4)  将 inner_code 映射到后缀模式--> 再转为前缀模式 
    后改为将 inner_code 直接在写死的字典中映射到入库所需的前缀模式 
    


### 指数日更新逻辑 

    根据查询语句： 
    select distinct(IndexCode) from datacenter.index_weight;
    
    进行日更新。
    
    更新的 code 信息位于 info_mixin.py 文件中。 