## Chapter 4. Asset-aware scheduling

So, we learned from previous chapter how to trigger dag within schedules. But sometimes multiple teams may be forced to build dag for every teams. Then we trigger them but, what if there is update in data, or api endpoint is changed, data inconsistency may occur here. Also DAGs try to send request to a single API to get data, so it may cause extra high costs and time to load. 

We think that there are 2 ways to solve it. Firstly, we can run consumer DAGs right after running producer DAG. But it may not be efficient. If data in producer isn't updated but consumers continue to be triggered every day. So, it may be cause of calculation of extra resources. In addition, delay may be available, for example producer is triggered from 2am to 3am , and consumer is triggered from 2.30am to 3.30 am, but today producer is triggered from 2am to 4am. so in this situation, while updating data, data of DAGs may be mismatched. 

So, first way isn't working, we are going to try second way: to trigger consumer DAGs directly. but it will not work well, it may create tight coupling between producer and consumer. 

So, we should apply asset-aware scheduling. Asset-aware allows you to define relationship between DAGs in terms of the assets they produce and consume. When you apply it, a DAG is triggered only when the changes are applied to assets. Assets connect producers and consumers. Producers generate or update the data (the output), while consumers are triggered by that update and use the resulting data as their input.  Assets may be data tables, API, ML Model, etc. 

Asset-aware scheduling doesn't follow time-based scheduling. 
What happens if you give logical date and asset at the same time? It will cause the mismatching of times of updating data. For example, you 've set a logical date at 3am, but your asset has updated at 4am. You can't know when the data was updated. So you should not give asset and time at the same time. 

triggering_asset_events => it provides the collection of events that triggered the DAG. If you want to grab only th first event of the asset you can give first | first in the code. 


**code example qoy!!!!!!!!

Giving metadata to Asset. You can give metadata tou assets that giving clear information about the asset and its updates. 

You can give multiple assets in the DAG. So how it works? For example you have some producers with different assets. Producer 1, Producer 2, and Consumer. Every producer has different asset. Firstly Producer 1 will be run but Consumer will wait for being run. Then Producer 2 will be run and finally Consumer will be run and DAG will be triggered completely.

If you want to apply time schedule and asset aware scheduling at the same time, you can use AssetorTimeSchedule. 

**code example qoy!!!!!