## Chapter5. Templating tasks using the Airflow context

In this chapter, we are going to summarize the concept with a Wikipedia example. Imagine you want to extract pageview counts — which page was viewed and how many times, by Wikipedia visitors. Wikipedia publishes this data hourly, so the URL of the file we need to download changes every hour (it contains the year, month, day, and hour). Since we can't hardcode the date manually every time, we need to build the URL dynamically, based on the date/time our DAG is running for. This is exactly where Jinja templating comes in — it allows us to inject dynamic values (such as date parameters) into a string at execution time, instead of writing them by hand. Without Jinja, we may be forced to write separate dags for every hours , but it is not practical. 

How to template it?
code example

```
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

```
get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,   # <-- we will call get_data
)
```

With BashOperator, we give a command as a string, and Jinja automatically templates that string. With PythonOperator, we give a function instead — and Jinja can't reach inside a function's code (only inside strings). So instead, Airflow passes all context variables (like the logical date) directly into the function as arguments — either by naming them explicitly  (e.g., def _get_data(logical_date):) or by capturing everything at once with **kwargs (def _get_data(**kwargs : then kwargs["logical_date"]). 

