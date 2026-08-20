## Chapter5. Templating tasks using the Airflow context

In this chapter, we are going to summarize the concept with a Wikipedia example. Imagine you want to extract pageview counts — which page was viewed and how many times, by Wikipedia visitors. Wikipedia publishes this data hourly, so the URL of the file we need to download changes every hour (it contains the year, month, day, and hour). Since we can't hardcode the date manually every time, we need to build the URL dynamically, based on the date/time our DAG is running for. This is exactly where Jinja templating comes in — it allows us to inject dynamic values (such as date parameters) into a string at execution time, instead of writing them by hand. Without Jinja, we may be forced to write separate dags for every hours , but it is not practical. 

How to template it?
code example

```python
get_data = BashOperator(
  task_id="get_data",
  bash_command=(
    "curl -o /tmp/wikipageviews.gz "
    "https://dumps.wikimedia.org/other/pageviews/"
    "{{ logical_date.year }}/"                         
    "{{ logical_date.year }}-"
    "{{ '{:02}'.format(logical_date.month) }}/"
    "pageviews-{{ logical_date.year }}"
    "{{ '{:02}'.format(logical_date.month) }}"
    "{{ '{:02}'.format(logical_date.day) }}-"
    "{{ '{:02}'.format(logical_date.hour) }}0000.gz"   
  ), 
  ```

  In this code, you can see that we used double curly bracket ({{ }}). Instead of writing all dates manually, we just set template, so it will work for any date time. 

We used the BashOperator, but PythonOperator can also be templated, though differently. In this approach, the python_callable argument tells Airflow which function to call ("callable" means "a function that can be called"). 

```python
get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,   # <-- we will call get_data
)
```

With BashOperator, we give a command as a string, and Jinja automatically templates that string. With PythonOperator, we give a function instead — and Jinja can't reach inside a function's code (only inside strings). So instead, Airflow passes all context variables (like the logical date) directly into the function as arguments — either by naming them explicitly  (e.g., def _get_data(logical_date):) or by capturing everything at once with **kwargs (def _get_data(**kwargs : then kwargs["logical_date"]). 

Or we can give a spesific parameter of **kwargs. 

For example: def get_data("logical_date", **kwargs)

We will use just logical_date parameter. 

### Passing additional variables to `PythonOperator`

`PythonOperator` allows us to pass additional arguments to the callable using `op_args` or `op_kwargs`.

**1. `op_args` — positional arguments**

```python
get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_args=["/tmp/wikipageviews.gz"],
)
```

```python
def _get_data(output_path, **context):
    ...
```

Equivalent to:

```python
_get_data("/tmp/wikipageviews.gz")
```

**2. `op_kwargs` — keyword arguments**

```python
get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    op_kwargs={"output_path": "/tmp/wikipageviews.gz"},
)
```

Equivalent to:

```python
_get_data(output_path="/tmp/wikipageviews.gz")
```

**3. Templated / dynamic values**

`op_kwargs` can contain Jinja templates:

```python
op_kwargs={
    "year": "{{ logical_date.year }}",
    "month": "{{ logical_date.month }}",
    "output_path": "/tmp/wikipageviews_{{ logical_date.format('YYYYMMDDHH') }}.gz",
}
```

The values are rendered dynamically based on the task's execution context.


How to inspect templated arguments? In Airflow UI, we can use Rendered Template button, but in terminal we can do it with `airflow tasks render` 

```bash
airflow tasks render [dag_id] [task_id] [desired execution date]
```
After executing this code you can see all templated arguments of task.
For example:

```bash
airflow tasks render 07_wikipedia_pageviews get_data "2026-08-18T13:00:00"
```
```
# ----------------------------------------------------------
# property: templates_dict
# ----------------------------------------------------------
None
# ----------------------------------------------------------
# property: op_args
# ----------------------------------------------------------
()
# ----------------------------------------------------------
# property: op_kwargs
# ----------------------------------------------------------
{'year': '2026', 'month': '8', 'day': '18', 'hour': '13', 'output_path': '/tmp/wikipageviews-2026081813.gz'}
```

### How to pass the data between tasks?
There are 2 ways to do it:

1. Use Airflow metastore to write and read results between tasks.(XCom)
2. Write results to and from a persistent location(such as a disk or database) between tasks. 

So before we just retrieved all hourly pageviews, but now we are going to retrieve the view count of every page
#### SQLExecuteQueryOperator

We want to overwrite to database instead of printing them. So we use SQLExecuteQueryOperator. It executes any SQL Query. 

For example
```python
write_to_postgres = SQLExecuteQueryOperator(
        task_id="write_to_postgres",
        conn_id="my_postgres",
        sql="postgres_query.sql",
        return_last=False,
    )
```

