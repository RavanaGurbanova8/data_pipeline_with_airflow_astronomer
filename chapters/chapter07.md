## Triggering workflows with external input

### Polling conditions with sensors.

Imagine that there are 4 supermarkets and a data analyst wants to analyze all 4 supermarkets' sales. So, data of every supermarket is required. The architecture will be as following:
```mermaid
graph LR
    Copy_raw_supermarket1 --> process_raw_supermarket1
    Copy_raw_supermarket2 --> process_raw_supermarket2
    Copy_raw_supermarket3 --> process_raw_supermarket3
    Copy_raw_supermarket4 --> process_raw_supermarket4
    process_raw_supermarket1 --> create_metrics
    process_raw_supermarket2 --> create_metrics
    process_raw_supermarket3 --> create_metrics
    process_raw_supermarket4 --> create_metrics
```

But, data is arrived in different time. For example, supermarket1 at 4pm, supermarket2 at 5pm , supermarket3 at half past 6, and supermarket4 at 9pm. And workflow is executing at 10pm. So, you should you FileSensor. It checks that data is exists or not. If exists, state will be return as True and workflow will work, in contrast, it will return False and will wait for data for any give time.
But workflow wait for all data for a long time, so we can start workflow before poking for the availability of given files. 
```mermaid
graph LR
    A[wait_raw_supermarket1<br>FileSensor<br>success] --> E[copy_raw_supermarket1<br>success]
    B[wait_raw_supermarket2<br>FileSensor<br>running] --> F[copy_raw_supermarket2]
    C[wait_raw_supermarket3<br>FileSensor<br>running] --> G[copy_raw_supermarket3]
    D[wait_raw_supermarket4<br>FileSensor<br>running] --> H[copy_raw_supermarket4]
    E[copy_raw_supermarket4<br>success] --> N[process_raw_supermarket1<br>success]
    F[copy_raw_supermarket4] --> R[process_raw_supermarket2]
    G[copy_raw_supermarket4] --> Q[process_raw_supermarket3]
    H[copy_raw_supermarket4] --> Z[process_raw_supermarket4]
    N --> create_metrics
    R --> create_metrics
    Q --> create_metrics
    Z --> create_metrics
```
#### Polling custom conditions

