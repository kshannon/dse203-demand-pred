import pytest
import json

from Mediator import Mediator
md = Mediator()

class TestUnfolding(object):

    def test_qry_train_nodeids(self):
        input_datalog = ('''Ans ( nodeid, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol,
    p12m_sales, p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating,
    p12m_numreviews, p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp ) :-
 mlfeatures ( nodeid, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol,
     p12m_sales, p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating,
     p12m_numreviews, p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp ) ,
     nodeid in (15, 45, 121)
''')

        output_datalog = Ans_1 = ('''Ans(nodeid,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp):-S1.mv_ml_features(nodeid,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating),S2.mlview(nodeid,yr,mn,pm_avgsntp,p3m_avgsntp,p12m_avgsntp),nodeid in (15,45,121)''')
        assert md.unfold_datalog(input_datalog) == output_datalog


    def test_dec_sales(self):
        input_datalog = ('''Ans ( nodeid, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales,
    p3m_vol, p12m_sales, p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews,
    p3m_avgrating, p12m_numreviews, p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp ) :-
 mlfeatures ( nodeid, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol, p12m_sales,
     p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating, p12m_numreviews,
     p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp ) ,
     nodeid in (15, 45, 121),
     mn=12,
     yr=2015
''')

        output_datalog = ('''Ans(nodeid,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp):-S1.mv_ml_features(nodeid,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating),S2.mlview(nodeid,yr,mn,pm_avgsntp,p3m_avgsntp,p12m_avgsntp),mn=12,yr=2015,nodeid in (15,45,121)''')

        assert md.unfold_datalog(input_datalog) == output_datalog


    def test_top_3_sales_xmas(self):
        input_datalog = ('''Ans ( nodeid, sales) :- sales_agg_mn(nodeid, mn, sales, _, _, rank_sales, _) ,
    mn=12,
    rank_sales<=3
''')

        output_datalog = ('''Ans(nodeid,sales):-S1.sales_agg_mn(nodeid,mn,sales,_,_,rank_sales,_),mn=12,rank_sales<=3''')

        assert md.unfold_datalog(input_datalog) == output_datalog


    def test_top_3_vol_last_yr(self):
        input_datalog = ('''Ans ( nodeid, sales,vol) :- sales_agg_mn(nodeid, mn, sales, vol, _, _, rank_vol) ,
    mn=12,
    rank_vol<=3
''')

        output_datalog = ('''Ans(nodeid,sales,vol):-S1.sales_agg_mn(nodeid,mn,sales,vol,_,_,rank_vol),mn=12,rank_vol<=3''')

        assert md.unfold_datalog(input_datalog) == output_datalog


    def test_top_3_sales_last_yr(self):
        input_datalog = ('''Ans ( nodeid, sales) :- sales_agg_yr(nodeid, yr, sales, _ , _ , rank_sales, _ ) ,
    yr = 2016,
    rank_sales<=3
''')

        output_datalog = ('''Ans(nodeid,sales):-S1.sales_agg_yr(nodeid,yr,sales,_,_,rank_sales,_),yr=2016,rank_sales<=3''')

        assert md.unfold_datalog(input_datalog) == output_datalog


    def test_too_many_atoms(self):
        input_datalog = ('''Ans ( nodeid, sales,vol) :- sales_agg_mn(nodeid, mn, sales, vol, _, _, rank_vol,_) ,
            mn=12,
            rank_vol<=3
        ''')

        output_datalog = (
        '''Ans(nodeid,sales,vol):-S1.sales_agg_mn(nodeid,mn,sales,vol,_,_,rank_vol),mn=12,rank_vol<=3''')

        with pytest.raises(Exception) as e:
            md.unfold_datalog(input_datalog) == output_datalog
        assert 'wrong number of atoms' in str(e.value)


    def test_too_few_atoms(self):
        input_datalog = ('''Ans ( nodeid, sales,vol) :- sales_agg_mn(nodeid, mn, sales, vol, rank_vol) ,
            mn=12,
            rank_vol<=3
        ''')

        output_datalog = (
            '''Ans(nodeid,sales,vol):-S1.sales_agg_mn(nodeid,mn,sales,vol,_,_,rank_vol),mn=12,rank_vol<=3''')

        with pytest.raises(Exception) as e:
            md.unfold_datalog(input_datalog) == output_datalog
        assert 'wrong number of atoms' in str(e.value)


    def test_head_att_not_in_body(self):
        input_datalog = ('''Ans ( nodeid, sales, vol, foo) :- sales_agg_mn(nodeid, mn, sales, vol, _, _, rank_vol) ,
            mn=12,
            rank_vol<=3
        ''')

        output_datalog = (
            '''Ans(nodeid,sales,vol):-S1.sales_agg_mn(nodeid,mn,sales,vol,_,_,rank_vol),mn=12,rank_vol<=3''')

        with pytest.raises(Exception) as e:
            md.unfold_datalog(input_datalog) == output_datalog
        assert ('head attribute' in str(e.value) and 'not in datalog body' in str(e.value))


    def test_optimize_query(self):
        input_datalog = ('''Ans ( nodeId, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol, p12m_sales, p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating, p12m_numreviews, p12m_avgrating) :-
 mlfeatures ( nodeId, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol, p12m_sales, p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating, p12m_numreviews, p12m_avgrating, _, _, _) , nodeId in (15, 45, 121)''')
        
        output_datalog = ('''Ans(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating):-S1.mv_ml_features(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating),nodeId in (15,45,121)''')
        
        assert md.unfold_datalog(input_datalog) == output_datalog

        
    def test_top_3_sales_xmas_multi_step(self):
        input_datalog = ('''Step1 (nodeId, sales):-order_by(sales_agg_mn (nodeId, mn, sales, _, _, _, _), [sales], [d]), mn=12.
Ans(nodeId, sales):-top(3, Step1(nodeId, sales))''')
        
        output_datalog = ('''sales_agg_mn_orderby_1(nodeId,mn,sales,_,_,_,_):-S1.sales_agg_mn(nodeId,mn,sales,_,_,_,_).Step1(nodeId,sales):-orderby(sales_agg_mn_orderby_1(nodeId,mn,sales,_,_,_,_),[sales],[d]),mn=12.Ans(nodeId,sales):-top(3,Step1(nodeId,sales))''')
        
        assert md.unfold_datalog(input_datalog) == output_datalog
        
        
    def test_top_3_sales_last_yr_multi_step(self):
        input_datalog = ('''Step1 (nodeId, sales) :- order_by(sales_agg_yr (nodeId, yr, sales,_,_,_,_), [sales], [d]), yr=2016.
Ans (nodeId, sales):-top(3, Step1(nodeId, sales))''')
        
        output_datalog = ('''sales_agg_yr_orderby_1(nodeId,yr,sales,_,_,_,_):-S1.sales_agg_yr(nodeId,yr,sales,_,_,_,_).Step1(nodeId,sales):-orderby(sales_agg_yr_orderby_1(nodeId,yr,sales,_,_,_,_),[sales],[d]),yr=2016.Ans(nodeId,sales):-top(3,Step1(nodeId,sales))''')
        
        assert md.unfold_datalog(input_datalog) == output_datalog
        
        
    def test_ml_features_orderby_sales(self):
        input_datalog = ('''Ans ( nodeId, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol, p12m_sales, 
     p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating, p12m_numreviews, 
     p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp ) :- 
order_by(mlfeatures ( nodeId, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol, p12m_sales, 
     p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating, p12m_numreviews, 
     p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp), [sales], [d]), yr=2016.''')
        
        output_datalog = ('''mlfeatures_orderby_1(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp):-S1.mv_ml_features(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating),S2.mlview(nodeId,yr,mn,pm_avgsntp,p3m_avgsntp,p12m_avgsntp).Ans(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp):-orderby(mlfeatures_orderby_1(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp),[sales],[d]),yr=2016''')
        
        assert md.unfold_datalog(input_datalog) == output_datalog

        
    def test_ml_features_top3_sales_2016_multi_step(self):
        input_datalog = ('''Step1 ( nodeId, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol, p12m_sales, 
     p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating, p12m_numreviews, 
     p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp ) :- 
order_by(mlfeatures ( nodeId, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol, p12m_sales, 
     p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating, p12m_numreviews, 
     p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp), [sales], [d]), yr=2016.
Ans ( nodeId, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol, p12m_sales,
     p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating, p12m_numreviews, 
     p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp) :- 
     top(3, Step1(nodeId, yr, mn, sales, vol, pm_sales, pm_vol, p3m_sales, p3m_vol, p12m_sales, 
     p12m_vol, pm_numreviews, pm_avgrating, p3m_numreviews, p3m_avgrating, p12m_numreviews, 
     p12m_avgrating, pm_avgsntp, p3m_avgsntp, p12m_avgsntp))''')
        
        output_datalog = ('''mlfeatures_orderby_1(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp):-S1.mv_ml_features(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating),S2.mlview(nodeId,yr,mn,pm_avgsntp,p3m_avgsntp,p12m_avgsntp).Step1(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp):-orderby(mlfeatures_orderby_1(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp),[sales],[d]),yr=2016.Ans(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp):-top(3,Step1(nodeId,yr,mn,sales,vol,pm_sales,pm_vol,p3m_sales,p3m_vol,p12m_sales,p12m_vol,pm_numreviews,pm_avgrating,p3m_numreviews,p3m_avgrating,p12m_numreviews,p12m_avgrating,pm_avgsntp,p3m_avgsntp,p12m_avgsntp))''')
        
        assert md.unfold_datalog(input_datalog) == output_datalog

        
    def test_group_by(self):
        input_datalog = ('''Ans ( nodeId, yr, mn, total_sales) :-
 group_by(sales_agg_yrmn ( nodeId, yr, mn, sales, _, _, _, _) , [nodeId, yr, mn], total_sales=sum(sales))
 ''')

        output_datalog = (
        '''sales_agg_yrmn_groupby_1(nodeId,yr,mn,sales,_,_,_,_):-S1.sales_agg_yrmn(nodeId,yr,mn,sales,_,_).Ans(nodeId,yr,mn,total_sales):-groupby(sales_agg_yrmn_groupby_1(nodeId,yr,mn,sales,_,_,_,_),[nodeId,yr,mn],total_sales=sum(sales))''')

        assert md.unfold_datalog(input_datalog) == output_datalog


    def test_order_by(self):
        input_datalog = ('''Ans ( nodeId, yr, mn, sales) :-
 order_by(sales_agg_yrmn ( nodeId, yr, mn, sales, _, _, _, _) , [sales], [d])
 ''')

        output_datalog = (
            '''sales_agg_yrmn_orderby_1(nodeId,yr,mn,sales,_,_,_,_):-S1.sales_agg_yrmn(nodeId,yr,mn,sales,_,_).Ans(nodeId,yr,mn,sales):-orderby(sales_agg_yrmn_orderby_1(nodeId,yr,mn,sales,_,_,_,_),[sales],[d])''')

        assert md.unfold_datalog(input_datalog) == output_datalog


    def test_top_n(self):
        input_datalog = ('''Ans ( nodeId, yr, mn, sales) :-
 top(3, sales_agg_yrmn ( nodeId, yr, mn, sales, _, _, _, _))
 ''')

        output_datalog = (
            '''sales_agg_yrmn_topn_1(nodeId,yr,mn,sales,_,_,_,_):-S1.sales_agg_yrmn(nodeId,yr,mn,sales,_,_).Ans(nodeId,yr,mn,sales):-top(3,sales_agg_yrmn_topn_1(nodeId,yr,mn,sales,_,_,_,_))''')

        assert md.unfold_datalog(input_datalog) == output_datalog

