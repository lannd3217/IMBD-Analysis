```python
# Initialize Otter
import otter
grader = otter.Notebook("proj1.ipynb")
```

# Project 1 - SQL
## Due Date: Thursday, September 21st, 5:00pm

In this project, we will be working with SQL on the IMDB database.

## Objectives

- Explore and extract relevant information from database with SQL functions
- Perform data cleaning and transformation using string functions and regex
- Use the cleaned data to run insightful analysis using joins, aggregations, and window functions

**Note:** If at any point during the project, the internal state of the database or its tables have been modified in an undesirable way (i.e. a modification not resulting from the instructions of a question), restart your kernel, clear output, and simply re-run the notebook as normal. This will shutdown your current connection to the database, which will prevent the issue of multiple connections to the database at any given point. When re-running the notebook, you will create a fresh database based on the provided Postgres dump.

## Logistics & Scoring Breakdown

Each coding question has **both public tests and hidden tests**. Roughly 50% of your grade will be made up of your score on the public tests released to you, while the remaining 50% will be made up of unreleased hidden tests. In addition, there are two free-response questions that will be manually graded.

This is an **individual project**. However, you’re welcome to collaborate with any other student in the class as long as it’s within the [academic honesty guidelines](https://fa23.data101.org/syllabus/#collaboration-and-integrity). Create new cells as needed to acknowledge others.

|Question|Points|
|---|---|
|0|1|
|1a|1|
|1b|2|
|1c|1|
|1d|1|
|2a|1|
|2b|3|
|2c|3|
|3a|2|
|3b|2|
|3c|2|
|3d|1|
|4a|2|
|4b|2|
|4c|1|
|5|2|
|**Total**|27|


```python
# Run this cell to set up imports
import numpy as np
import pandas as pd
```

<hr style="border: 5px solid #003262;" />
<hr style="border: 1px solid #fdb515;" />

# Before You Start: Assignment Tips

<div class="alert alert-block alert-info">
<p><b>Please Read!!</b> In this project we will assume you have attended lecture and seen how to connect to a Postgres server via two ways: JupySQL in Jupyter Notebook, and the psql command-line program.</p>
</div>
    
We have written up these instructions for you in the <a href="https://fa23.data101.org/resources/assignment-tips/">Fall 2023 Assignment Tips</a>—a handy resource that has many other tips:

* PostgreSQL documentation
* JupySQL and magic commands in Jupyter
* JupyterHub keyboard shortcuts
* psql and common meta-commands
* Debugging:
    * Where to create new cells to play nice with the autograder
    * Opening/closing connections, deleting databases if all else fails
* Local installation (not supported by staff officially, but for your reference)



For some questions with multi-line cell magic, we will also be saving the literal query string with [query snippets](https://jupysql.ploomber.io/en/latest/api/magic-snippets.html) using `--save`:

``%%sql --save query result << select * FROM table ...``

# Database Setup
We are going to be using the `JupySQL` library to connect our notebook to a PostgreSQL database server on your JupyterHub account. Running the next cell will do so; you should not see any error messages after it executes.


```python
# The first time you are running this cell, you may need to run the following line as: %load_ext sql 
%reload_ext sql
```

In the next cell, we will unzip the data. This only needs to be done once.


```python
!unzip -u data/imdbdb.zip -d data/
```

    Archive:  data/imdbdb.zip


<br/>

**Create the `imdb` database**: <br>
We will use PostgreSQL commands to create a database and import our data into it. Run the following cell to do this.
* You can also run these cells in the command-line via `psql`.
* If you run into the **role does not exist** error, feel free to ignore it. It does not affect data import.


```python
!psql postgresql://jovyan@127.0.0.1:5432/imdb -c 'SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = current_database()  AND pid <> pg_backend_pid();'
!psql postgresql://jovyan@127.0.0.1:5432/postgres -c 'DROP DATABASE IF EXISTS imdb'
!psql postgresql://jovyan@127.0.0.1:5432/postgres -c 'CREATE DATABASE imdb'
!psql postgresql://jovyan@127.0.0.1:5432/imdb -f data/imdbdb.sql
```

     pg_terminate_backend 
    ----------------------
     t
    (1 row)
    
    DROP DATABASE
    CREATE DATABASE
    SET
    SET
    SET
    SET
    SET
     set_config 
    ------------
     
    (1 row)
    
    SET
    SET
    SET
    SET
    SET
    SET
    CREATE TABLE
    ALTER TABLE
    CREATE TABLE
    ALTER TABLE
    CREATE TABLE
    ALTER TABLE
    CREATE TABLE
    ALTER TABLE
    CREATE TABLE
    ALTER TABLE
    CREATE TABLE
    ALTER TABLE
    COPY 500000
    COPY 3804162
    COPY 113
    COPY 2433431
    COPY 337179
    COPY 12
    ALTER TABLE
    ALTER TABLE


**Connect to `imdb` database in the Notebook**: 
<br>
Now let's connect to the new database we just created! There should be no errors after running the following cell.


```python
%sql postgresql://jovyan@127.0.0.1:5432/imdb
```

**Connect to `imdb` database in `psql`**: 

<div class="alert alert-block alert-info">
<b>Do the following in a Terminal window!</b>
</div>

Connect to the same database via `psql`. See the [Fall 2023 Assignment Tips](https://fa23.data101.org/resources/assignment-tips/) website resource for details on connecting. Run the following meta-command in the `psql` client:

``
\l
``

This should display all databases on this server, including the `imdb` database you just created.


---

**Quick check**: To make sure things are working, let's fetch 10 rows from one of our tables `cast_sample`. Just run the following cell, no further action is needed.


```sql
%%sql
SELECT * 
  FROM cast_sample
LIMIT 10
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>


    /srv/conda/envs/notebook/lib/python3.11/site-packages/sql/connection/connection.py:827: JupySQLRollbackPerformed: Server closed connection. JupySQL executed a ROLLBACK operation.
      warnings.warn(



<span style="color: green">10 rows affected.</span>





<table>
    <thead>
        <tr>
            <th>id</th>
            <th>person_id</th>
            <th>movie_id</th>
            <th>role_id</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>708</td>
            <td>235</td>
            <td>2345369</td>
            <td>1</td>
        </tr>
        <tr>
            <td>721</td>
            <td>241</td>
            <td>2504309</td>
            <td>1</td>
        </tr>
        <tr>
            <td>789</td>
            <td>264</td>
            <td>2156734</td>
            <td>1</td>
        </tr>
        <tr>
            <td>875</td>
            <td>299</td>
            <td>1954994</td>
            <td>1</td>
        </tr>
        <tr>
            <td>888</td>
            <td>302</td>
            <td>765037</td>
            <td>1</td>
        </tr>
        <tr>
            <td>889</td>
            <td>302</td>
            <td>765172</td>
            <td>1</td>
        </tr>
        <tr>
            <td>898</td>
            <td>306</td>
            <td>291387</td>
            <td>1</td>
        </tr>
        <tr>
            <td>899</td>
            <td>306</td>
            <td>1477434</td>
            <td>1</td>
        </tr>
        <tr>
            <td>931</td>
            <td>324</td>
            <td>824119</td>
            <td>1</td>
        </tr>
        <tr>
            <td>1936</td>
            <td>543</td>
            <td>1754068</td>
            <td>1</td>
        </tr>
    </tbody>
</table>
<span style="font-style:italic;text-align:center;">Truncated to <a href="https://jupysql.ploomber.io/en/latest/api/configuration.html#displaylimit">displaylimit</a> of 10.</span>



## Connect to the grader


```python
# Connecting the grader
# Just run the following cell, no further action is needed.
from data101_utils import GradingUtil
grading_util = GradingUtil("proj1")
grading_util.prepare_autograder()
```

## The `imdb` Database

In this project, we are working with a reduced version of the Internet Movie Database (IMDb) database. This Postgres database is a small random sample of actors from the much larger full database (which is over several GBs large) and includes their corresponding movies and cast info. Disclaimer: as a result, we may obtain wildly  different results than if we were to use the entire database. 

- **actor_sample**: information about the actors including id, name, and gender
- **cast_sample**: each person on the cast of each movie gets a row including cast id, each person's id (`actor_sample.id`), movie id (`movie_sample.id`), and role id
- **movie_sample**: sample of movies the actors have been in, including movie id, title, and the production year
- **movie_info_sample**: this table originally had a lot of information for each movie (take a look at info_type to see the information available), but we have dropped some information to make it easier to manage. This table includes movie info's id, movie id, info type id, and the info itself
- **info_type**: reference table to match each info type id to the description of the type of information
- **role_type**: reference table for cast_sample to match role id to the description of the role

### Key Notes
- This database is **not** the same as the IMDb lecture database, but has a lot of of similar features. 
- Point of confusion: `movie_sample` and `actor_sample` both have attributes `id` corresponding to 7 digit unique numeric identifiers, but do **not** refer to the same data values.
- `cast_sample` is analagous to the `crew` table from lecture. It can be used to match an actor's id to movies they have acted in, among other relations.
- You are highly encouraged to spend some time exploring the metadata of these tables using Postgres meta-commands to better understand the data given and the relations between tables.

<br/><br/><br/>

<hr style="border: 1px solid #fdb515;" />

# The `information_schema` schema

A **schema** is a namespace of tables in the database, often used for security purposes. Let's see how many schema are defined for us in our current database:



```sql
%%sql
SELECT * 
FROM information_schema.schemata;
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">4 rows affected.</span>





<table>
    <thead>
        <tr>
            <th>catalog_name</th>
            <th>schema_name</th>
            <th>schema_owner</th>
            <th>default_character_set_catalog</th>
            <th>default_character_set_schema</th>
            <th>default_character_set_name</th>
            <th>sql_path</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>imdb</td>
            <td>pg_toast</td>
            <td>jovyan</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>imdb</td>
            <td>pg_catalog</td>
            <td>jovyan</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>imdb</td>
            <td>public</td>
            <td>jovyan</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
        <tr>
            <td>imdb</td>
            <td>information_schema</td>
            <td>jovyan</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
        </tr>
    </tbody>
</table>



Within a Postgres database, there are often at least three schemas:
* `public`, a public schema that users can access and create tables in;
* `pg_catalog`, a schema for maintaining system information; and
* `information_schema`, a schema that maintains metadata about objects currently created in the database.
* The fourth schema `pg_toast` maintains data that can't regularly be stored in relations, such as very large data values. See more in documentation [here](https://www.postgresql.org/docs/current/storage-toast.html).

For now, we focus on the `information_schema` schemata, which stores our metadata. That’s right—metadata is also data, and as we make updates to our `public` databases, metadata is automatically stored and updated into different tables under the `information_schema` schema.

There are many metadata tables that Postgres updates for us, and the full list is in the Postgres documentation [(Chapter 37)](https://www.postgresql.org/docs/current/information-schema.html). 
For now, let’s look at which the `.tables` table ([37.54](https://www.postgresql.org/docs/current/infoschema-tables.html)), which lists all the tables located in the database. Let’s specifically look at those that are in the public schema (i.e., publicly accessible tables):


```sql
%%sql
SELECT * 
FROM information_schema.tables
WHERE table_schema = 'public';
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">6 rows affected.</span>





<table>
    <thead>
        <tr>
            <th>table_catalog</th>
            <th>table_schema</th>
            <th>table_name</th>
            <th>table_type</th>
            <th>self_referencing_column_name</th>
            <th>reference_generation</th>
            <th>user_defined_type_catalog</th>
            <th>user_defined_type_schema</th>
            <th>user_defined_type_name</th>
            <th>is_insertable_into</th>
            <th>is_typed</th>
            <th>commit_action</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>imdb</td>
            <td>public</td>
            <td>actor_sample</td>
            <td>BASE TABLE</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>YES</td>
            <td>NO</td>
            <td>None</td>
        </tr>
        <tr>
            <td>imdb</td>
            <td>public</td>
            <td>cast_sample</td>
            <td>BASE TABLE</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>YES</td>
            <td>NO</td>
            <td>None</td>
        </tr>
        <tr>
            <td>imdb</td>
            <td>public</td>
            <td>info_type</td>
            <td>BASE TABLE</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>YES</td>
            <td>NO</td>
            <td>None</td>
        </tr>
        <tr>
            <td>imdb</td>
            <td>public</td>
            <td>movie_info_sample</td>
            <td>BASE TABLE</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>YES</td>
            <td>NO</td>
            <td>None</td>
        </tr>
        <tr>
            <td>imdb</td>
            <td>public</td>
            <td>movie_sample</td>
            <td>BASE TABLE</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>YES</td>
            <td>NO</td>
            <td>None</td>
        </tr>
        <tr>
            <td>imdb</td>
            <td>public</td>
            <td>role_type</td>
            <td>BASE TABLE</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>None</td>
            <td>YES</td>
            <td>NO</td>
            <td>None</td>
        </tr>
    </tbody>
</table>



<br/><br/><br/>

<hr style="border: 1px solid #fdb515;" />


# Question 0


As stated above, there are many metadata tables stored in the `information_schema` schema. Write a query that returns the names of all relations in the PostgreSQL `information_schema` schema, i.e., the names of all the metadata tables

**Hints:**
* Your resulting table names should correspond to what’s listed in the information schema documentation [(Chapter 37)](https://www.postgresql.org/docs/15/information-schema.html). 
* For you to think about: Why might there be fewer tables in your query response than the full list in the documentation? 


```sql
%%sql --save query_0 result_0 <<
SELECT table_name 
FROM information_schema.tables
WHERE table_schema = 'information_schema';
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">69 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_0 = %sqlcmd snippets query_0
grading_util.save_results("result_0", query_0, result_0)
result_0
```




<table>
    <thead>
        <tr>
            <th>table_name</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>information_schema_catalog_name</td>
        </tr>
        <tr>
            <td>attributes</td>
        </tr>
        <tr>
            <td>applicable_roles</td>
        </tr>
        <tr>
            <td>administrable_role_authorizations</td>
        </tr>
        <tr>
            <td>check_constraint_routine_usage</td>
        </tr>
        <tr>
            <td>character_sets</td>
        </tr>
        <tr>
            <td>check_constraints</td>
        </tr>
        <tr>
            <td>collations</td>
        </tr>
        <tr>
            <td>collation_character_set_applicability</td>
        </tr>
        <tr>
            <td>column_column_usage</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q0")
```




<p><strong><pre style='display: inline;'>q0</pre></strong> passed! 🍀</p>



<br/><br/><br/>

<hr style="border: 1px solid #fdb515;" />


# Question 1: Exploratory Data Analysis
One of the first things you'll want to do with a database table is get a sense for its metadata: column names and types, and number of rows. 

## Tutorial 

We can use the PostgreSQL `\d` meta-command to get a description of all the columns in the `movie_info_sample` table. Open up a terminal window, connect to the imdb server, and analyze the output of the meta-command:

``\d movie_info_sample``

We can use the PostgreSQL `\d` meta-command to get a description the `movie_info_sample` schema. Open up a terminal window, connect to the imdb server, and analyze the output of the meta-command:

``\d movie_info_sample``

There are four attributes in this schema, of which `"id"` is one. What are the other attribute names? Assign `result_1a` to a list of strings, where each element is an attribute name. The list does not need to be in order.


```python
result_1a = ["id", "movie_id", "info_type_id", "info"]
result_1a
```




    ['id', 'movie_id', 'info_type_id', 'info']




```python
grader.check("q1a")
```




<p><strong><pre style='display: inline;'>q1a</pre></strong> passed! 🌈</p>



**Debugging tip**: Throughout this project and when working with databases, you should always be checking schemas via the `\d` psql metacommand.

<br><br>

---

## Question 1b

Next, let’s continue with our initial exploration of this table. How many rows are in this table? 

Assign `result_1b` to the result of a SQL query to calculate the number of rows in the `movie_info_sample` table. Then, assign `count_1b` to the integer number of rows based on what you found in `result_1b`. Do not hard code this value.

**Hints:**
- See the [Assignment Tips](https://fa23.data101.org/resources/assignment-tips/) page for how to use SQL line magic.
- Your query result should have exactly one row and one attribute; the lone value in the instance should be the number of rows.
- See the `JupySQL` [documentation](https://jupysql.ploomber.io/en/latest/intro.html) for how to index into a SQL query result.


```python
result_1b = %sql SELECT COUNT(*) FROM movie_info_sample; 
# replace with %sql command
count_1b = result_1b[0][0]

# do not edit below this line
display(result_1b)
count_1b
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">1 rows affected.</span>



<table>
    <thead>
        <tr>
            <th>count</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2433431</td>
        </tr>
    </tbody>
</table>





    2433431




```python
grader.check("q1b")
```




<p><strong><pre style='display: inline;'>q1b</pre></strong> passed! 🍀</p>



<br><br>

---

## Question 1c: Random table sample

Now that we know a bit about the metadata of the table, let's randomly sample rows from `movie_info_sample` to explore its contents.

Given that you know the size of the table from the previous query, **write a query that retrieves 5 tuples on expectation using the `BERNOULLI` sampling method.** That is, if we run the query multiple times, we should get 5 tuples on average in our resulting table. The `BERNOULLI` sampling method scans the whole table and selects individual rows independently with `p%` probability. Please see the [documentation](https://www.postgresql.org/docs/15/sql-select.html) for syntax.


**Hints/Details:**
* Assign `p_1c` to a sampling rate that you pass into the `query_1c` f-string using Python variable substitution. Your formula should contain `count_1b`. Don't forget to express `p_1c` in units of percent, i.e., `p_1c = 0.03` is 0.03%!
* For a refresher on f-strings and Python variable substitution, see [this tutorial](https://www.geeksforgeeks.org/formatted-string-literals-f-strings-python/). If Python variable substitution is done correctly, we should be able to change our `p%` probability by simply reassigning `p_1c` and rerunning the query. (Please leave `p_1c` unchanged.)
* We have completed the SQL line magic for you; this references the Python f-string `query_1c` you created within a SQL query using JupySQL-specific syntax.
* Try running the SQL cell many times and see what you notice.


```python
p_1c = 5/count_1b*100
query_1c = f"SELECT * FROM movie_info_sample TABLESAMPLE BERNOULLI({p_1c});" 
# edit this query string

# Do not edit below this line
result_1c = %sql {{query_1c}}
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">2 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
grading_util.save_results("result_1c", query_1c, result_1c)
result_1c
```




<table>
    <thead>
        <tr>
            <th>id</th>
            <th>movie_id</th>
            <th>info_type_id</th>
            <th>info</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>4398443</td>
            <td>1024138</td>
            <td>8</td>
            <td>France</td>
        </tr>
        <tr>
            <td>1474910</td>
            <td>1999078</td>
            <td>105</td>
            <td>$350,000</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q1c")
```




<p><strong><pre style='display: inline;'>q1c</pre></strong> passed! ✨</p>



<br><br>

---

## Question 1d: Random sample, fixed number of rows

If a random number of rows is not of importance, a more efficient way to get some arbitrary tuples from a table is to use the `ORDER BY` and `LIMIT` clauses. In the next cell, fetch 5 **random** tuples from `movie_info_sample`. Compared to the previous question, your query result here should always have 5 tuples!

**Hint**: Check out lecture.



```sql
%%sql --save query_1d result_1d <<
SELECT * FROM movie_info_sample ORDER BY RANDOM() LIMIT 5;
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">5 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_1d = %sqlcmd snippets query_1d
grading_util.save_results("result_1d", query_1d, result_1d)
result_1d
```




<table>
    <thead>
        <tr>
            <th>id</th>
            <th>movie_id</th>
            <th>info_type_id</th>
            <th>info</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1631874</td>
            <td>1102704</td>
            <td>105</td>
            <td>$250,000</td>
        </tr>
        <tr>
            <td>9722642</td>
            <td>2434417</td>
            <td>1</td>
            <td>60</td>
        </tr>
        <tr>
            <td>9503842</td>
            <td>2098373</td>
            <td>1</td>
            <td>7</td>
        </tr>
        <tr>
            <td>4903218</td>
            <td>1974739</td>
            <td>8</td>
            <td>Italy</td>
        </tr>
        <tr>
            <td>4831673</td>
            <td>1906933</td>
            <td>8</td>
            <td>Japan</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q1d")
```




<p><strong><pre style='display: inline;'>q1d</pre></strong> passed! 🌟</p>



<br/><br/><br/>
<hr style="border: 1px solid #fdb515;" />


# Question 2: Data Cleaning

The `movie_sample` table contains a very minimal amount of information per movie:


```python
%sql SELECT * FROM movie_sample LIMIT 5;
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">5 rows affected.</span>





<table>
    <thead>
        <tr>
            <th>id</th>
            <th>title</th>
            <th>production_year</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2038405</td>
            <td>La corte de faraón</td>
            <td>1944</td>
        </tr>
        <tr>
            <td>2081186</td>
            <td>Long de xin</td>
            <td>1985</td>
        </tr>
        <tr>
            <td>2177749</td>
            <td>Onésime aime les bêtes</td>
            <td>1913</td>
        </tr>
        <tr>
            <td>1718608</td>
            <td>Bedtime Worries</td>
            <td>1933</td>
        </tr>
        <tr>
            <td>2130699</td>
            <td>Mothman</td>
            <td>2000</td>
        </tr>
    </tbody>
</table>



In this question, we’re going to create a nice, refined view of the `movie_sample` table that also includes a **rating field**, called `movie_ratings`.

The [MPAA rating](https://www.motionpictures.org/film-ratings/) is commonly included in most datasets about movies, including ours, but in its current format in the dataset, it’s quite difficult to extract.

The first clue about our approach comes from the random rows you explored in Question 1. As you saw, the `movie_info_sample` table contains a lot of information about each movie. Each row contains a particular type of information (e.g., runtime, languages) categorized by `info_type_id`. Based on the other tables in this database, the `info_type` table is a reference table to this 
ID number.

Our strategy in this question is therefore as follows:
* **Question 2a**: Find the `mpaa_rating_id` from the `info_type` table.
* **Question 2b**: Extract the MPAA rating of a specific movie from the `movie_info_sample` table.
* **Question 2c**: Construct a view `movie_ratings` based on the `movie_sample` table and all relevant MPAA ratings extracted from the `movie_info_sample` table.

<br><br>

---

## Question 2a: MPAA Rating and `info_type`

To start, using the `info_type` table, write a query to find which `id` corresponds to a film's MPAA rating.  The query `result_2a` that you write should return a relation with exactly one row and one attribute; the lone value in the instance should be the MPAA rating id number. We've then assigned `mpaa_rating_id` to extract the number itself from the relation.

**Hints:** 
- Open the `psql` client in a terminal to explore the schema of `info_type` via the `\d` metacommand (see the [Assignment Tips](https://fa23.data101.org/resources/assignment-tips/) page). Remember you can also write SQL commands to that terminal to interact with the IMDB database, but all final work must be submitted through this Jupyter Notebook.
- Be careful when using quotes. SQL interprets single and double quotes differently. The single quote character `'` is reserved for delimiting string constants, while the double quote `"` is used for naming tables or columns that require special characters. See [documentation](https://www.postgresql.org/docs/current/sql-syntax-lexical.html) for more.


```python
result_2a = %sql SELECT id FROM info_type WHERE info= 'mpaa';
# replace with %sql command
mpaa_rating_id = result_2a[0][0]

# do not edit below this line
display(result_2a)
mpaa_rating_id
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">1 rows affected.</span>



<table>
    <thead>
        <tr>
            <th>id</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>97</td>
        </tr>
    </tbody>
</table>





    97




```python
grader.check("q2a")
```




<p><strong><pre style='display: inline;'>q2a</pre></strong> passed! 🙌</p>



<br><br>

---

## Question 2b: Looking up the MPAA Rating

Suppose we wanted to find the MPAA rating for the 2004 American teen drama classic, _Mean Girls_. The below cell assigns `movie_id_2b` to the IMDb ID of this movie, 2109683.


```python
# Just run this cell, no further action is needed.
movie_id_2b = 2109683
%sql SELECT * FROM movie_sample WHERE id = {{movie_id_2b}};
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">1 rows affected.</span>





<table>
    <thead>
        <tr>
            <th>id</th>
            <th>title</th>
            <th>production_year</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2109683</td>
            <td>Mean Girls</td>
            <td>2004</td>
        </tr>
    </tbody>
</table>



In the next cell, **write a query to find the MPAA rating for this movie.** Your query should return a relation with exactly one row , which has `(info, mpaa_rating)`, where `info` is the full MPAA rating string from `movie_info_sample`, and `mpaa_rating` is just the rating itself (i.e. `R`, `PG-13`, `PG`, etc) for this movie.


**Before you get started**:
* Explore the `movie_info_sample` tuples corresponding to the MPAA rating by using metacommands in the terminal. The `info` field is a little longer than just the rating. It also includes an explanation for why that movie received its rating. 
* You will need to extract a substring from the `info` column of `movie_info_sample`; you can use the [string functions](https://www.postgresql.org/docs/current/functions-string.html) in PostgreSQL to do it. There are many possible solutions. One possible solution is to use the substring function along with regex. If you use this approach, [this section on regex](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP) may be particularly useful. [regex101.com](https://regex101.com) may also be helpful to craft your regular expressions.
* You may use `mpaa_rating_id` and `movie_id_2b` directly in the rest of the questions using Python variable substitution (i.e., double curly braces). See the `JupySQL` documentation for more details.


```sql
%%sql --save query_2b result_2b << 
SELECT info, REGEXP_REPLACE(info, '^\w+ ([a-zA-Z|\d|-]+) .*$', '\1') AS mpaa_rating
FROM movie_info_sample
WHERE movie_id = {{movie_id_2b}} AND info_type_id = {{mpaa_rating_id}};
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">1 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_2b = %sqlcmd snippets query_2b
grading_util.save_results("result_2b", query_2b, result_2b)
result_2b
```




<table>
    <thead>
        <tr>
            <th>info</th>
            <th>mpaa_rating</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>Rated PG-13 for sexual content, language and some teen partying</td>
            <td>PG-13</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q2b")
```




<p><strong><pre style='display: inline;'>q2b</pre></strong> passed! 🍀</p>



You may use `mpaa_rating_id` directly in the rest of the questions using python variable substitution.

<br><br>

---

## Question 2c
In the next cell,
1. Construct a view named `movie_ratings` containing one row for each movie, which has `(movie_id, title, info, mpaa_rating)`, where `info` is the full MPAA rating string from `movie_info_sample`, and `mpaa_rating` is just the rating itself (i.e. `R`, `PG-13`, `PG`, etc).
    * In other words, extend `movie_sample` with the MPAA rating attributes that you found in the previous question part, but this time for all movies.
2. Following the view definition, also write a `SELECT` query to return the **first 20 rows** of the view, ordered by ascending `movie_id`.


```sql
%%sql --save query_2c result_2c << 
DROP VIEW IF EXISTS movie_ratings;
CREATE VIEW movie_ratings AS (
    SELECT movie_id, title, info, REGEXP_REPLACE(info, '^\w+ ([a-zA-Z|\d|-]+) .*$', '\1') AS mpaa_rating
    FROM movie_info_sample 
        INNER JOIN movie_sample 
            ON movie_info_sample.movie_id = movie_sample.id
    WHERE movie_info_sample.info_type_id = {{mpaa_rating_id}}
);

SELECT * FROM movie_ratings ORDER BY movie_id ASC LIMIT 20;


```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">20 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_2c = %sqlcmd snippets query_2c
grading_util.save_results("result_2c", query_2c, result_2c)
result_2c
```




<table>
    <thead>
        <tr>
            <th>movie_id</th>
            <th>title</th>
            <th>info</th>
            <th>mpaa_rating</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1632926</td>
            <td>$5 a Day</td>
            <td>Rated PG-13 for sexual content, brief nudity and language</td>
            <td>PG-13</td>
        </tr>
        <tr>
            <td>1632941</td>
            <td>$9.99</td>
            <td>Rated R for language and brief sexuality and nudity</td>
            <td>R</td>
        </tr>
        <tr>
            <td>1632956</td>
            <td>$windle</td>
            <td>Rated R for some violence and brief sexuality/nudity</td>
            <td>R</td>
        </tr>
        <tr>
            <td>1633013</td>
            <td>'A' gai wak</td>
            <td>Rated PG-13 for violence</td>
            <td>PG-13</td>
        </tr>
        <tr>
            <td>1633014</td>
            <td>'A' gai wak juk jap</td>
            <td>Rated PG-13 for violence</td>
            <td>PG-13</td>
        </tr>
        <tr>
            <td>1633461</td>
            <td>'R Xmas</td>
            <td>Rated R for strong language, drug content and some violence</td>
            <td>R</td>
        </tr>
        <tr>
            <td>1633618</td>
            <td>'Til There Was You</td>
            <td>Rated PG-13 for sensuality, language and drug references</td>
            <td>PG-13</td>
        </tr>
        <tr>
            <td>1633729</td>
            <td>(500) Days of Summer</td>
            <td>Rated PG-13 for sexual material and language</td>
            <td>PG-13</td>
        </tr>
        <tr>
            <td>1633856</td>
            <td>(Untitled)</td>
            <td>Rated R for language and nude images</td>
            <td>R</td>
        </tr>
        <tr>
            <td>1634282</td>
            <td>.45</td>
            <td>Rated R for pervasive strong language including graphic sexual references, violence, sexuality and some drug use</td>
            <td>R</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q2c")
```




<p><strong><pre style='display: inline;'>q2c</pre></strong> passed! 🚀</p>



<br/><br/><br/>

<hr style="border: 1px solid #fdb515;" />

# Question 3: Movie Moola
One measure of a movie's success is how much money it makes. If we look at our `info_type` table, we have information about the film's gross earnings and the budget for a film. It would be nice to know how much money a film made using the profit formula:
$$profit = earnings - moneyspent$$

We start by taking a look at the gross info type, with `info_type_id = 107`.


```sql
%%sql
SELECT * 
FROM movie_info_sample
WHERE info_type_id = 107
ORDER BY id
LIMIT 10 OFFSET 100000;
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">10 rows affected.</span>





<table>
    <thead>
        <tr>
            <th>id</th>
            <th>movie_id</th>
            <th>info_type_id</th>
            <th>info</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1464348</td>
            <td>2281091</td>
            <td>107</td>
            <td>INR 23,373,000 (India) (25 February 2005)</td>
        </tr>
        <tr>
            <td>1464349</td>
            <td>2281091</td>
            <td>107</td>
            <td>INR 19,207,000 (India) (18 February 2005)</td>
        </tr>
        <tr>
            <td>1464374</td>
            <td>1766950</td>
            <td>107</td>
            <td>HKD 826,364 (Hong Kong) (11 December 1975)</td>
        </tr>
        <tr>
            <td>1464375</td>
            <td>1769023</td>
            <td>107</td>
            <td>HKD 3,148,549 (Hong Kong) (19 November 1980)</td>
        </tr>
        <tr>
            <td>1464378</td>
            <td>1799099</td>
            <td>107</td>
            <td>HKD 6,493,694 (Hong Kong) (22 December 1981)</td>
        </tr>
        <tr>
            <td>1464383</td>
            <td>1847670</td>
            <td>107</td>
            <td>$21,438 (USA) (9 August 2009)</td>
        </tr>
        <tr>
            <td>1464384</td>
            <td>1847670</td>
            <td>107</td>
            <td>$10,266 (USA) (2 August 2009)</td>
        </tr>
        <tr>
            <td>1464396</td>
            <td>1916002</td>
            <td>107</td>
            <td>$5,932 (USA) (27 November 2005)</td>
        </tr>
        <tr>
            <td>1464397</td>
            <td>1916002</td>
            <td>107</td>
            <td>$4,206 (USA) (20 November 2005)</td>
        </tr>
        <tr>
            <td>1464398</td>
            <td>1916002</td>
            <td>107</td>
            <td>$2,939 (USA) (23 October 2005)</td>
        </tr>
    </tbody>
</table>
<span style="font-style:italic;text-align:center;">Truncated to <a href="https://jupysql.ploomber.io/en/latest/api/configuration.html#displaylimit">displaylimit</a> of 10.</span>



There are a lot of things to notice here. First of all, the values in the `info` attribute are strings with not only the earnings, but also the country and the month the earnings are cummulatively summed until. Additionally, the info values are not all in the same currency! On top of that, it appears as if some of the gross earnings, even for those in USD are from worldwide sales, while others only count sales within the USA.

For consistency, let's only use movies with gross earnings counted in the USA and that are in US Dollars ($). 

<br><br>

---

## Question 3a: Earnings
We want the numerical part of the `info` column and the **maximum earnings value** for a particular film. 

In the next cell,
- Construct a view named `movie_gross` containing one row for each movie, which has `(gross, movie_id, title)`, where `gross` is the numeric dollar amount extracted as a float.
- To take a look at our cleaned data, write a `SELECT` query to display the **top 10 highest grossing films** from `movie_gross`.

**Hints:** 
- The way we extracted the MPAA rating is very similar to how we want to isolate the numeric dollar amount as a string. (There are multiple ways of doing this.)
- Look at the [documentation](https://www.postgresql.org/docs/9.4/functions-matching.html) for the `regexp_replace` function, and specifically 'flag g'.
- The staff solution found it helpful to make an additional subview.


```sql
%%sql --save query_3a result_3a <<

DROP VIEW IF EXISTS raw_gross CASCADE;
CREATE VIEW raw_gross AS(
    SELECT CAST(REGEXP_REPLACE(info, ',|\$| \(.*\)', '', 'g') AS FLOAT) AS gross, movie_id, m.title AS title
    FROM movie_info_sample as i
    INNER JOIN movie_sample as m
        ON i.movie_id = m.id
    WHERE info_type_id = 107 
            AND info LIKE '%(USA)%' 
            AND info LIKE '%$%'
);
DROP VIEW IF EXISTS movie_gross;
CREATE VIEW movie_gross AS(
    SELECT MAX(gross) AS gross, movie_id, title
    FROM raw_gross
    GROUP BY movie_id, title
);

SELECT *
FROM movie_gross
ORDER BY gross DESC
LIMIT 10;

```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">10 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_3a = %sqlcmd snippets query_3a
grading_util.save_results("result_3a", query_3a, result_3a)
result_3a
```




<table>
    <thead>
        <tr>
            <th>gross</th>
            <th>movie_id</th>
            <th>title</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>760507625.0</td>
            <td>1704289</td>
            <td>Avatar</td>
        </tr>
        <tr>
            <td>658672302.0</td>
            <td>2438179</td>
            <td>Titanic</td>
        </tr>
        <tr>
            <td>623357910.0</td>
            <td>2346436</td>
            <td>The Avengers</td>
        </tr>
        <tr>
            <td>534858444.0</td>
            <td>2360583</td>
            <td>The Dark Knight</td>
        </tr>
        <tr>
            <td>460935665.0</td>
            <td>2310522</td>
            <td>Star Wars</td>
        </tr>
        <tr>
            <td>448139099.0</td>
            <td>2360588</td>
            <td>The Dark Knight Rises</td>
        </tr>
        <tr>
            <td>436471036.0</td>
            <td>2285018</td>
            <td>Shrek 2</td>
        </tr>
        <tr>
            <td>435110554.0</td>
            <td>1851357</td>
            <td>E.T. the Extra-Terrestrial</td>
        </tr>
        <tr>
            <td>431065444.0</td>
            <td>2310573</td>
            <td>Star Wars: Episode I - The Phantom Menace</td>
        </tr>
        <tr>
            <td>423315812.0</td>
            <td>2204345</td>
            <td>Pirates of the Caribbean: Dead Man's Chest</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q3a")
```




<p><strong><pre style='display: inline;'>q3a</pre></strong> passed! 🎉</p>



<br/>

---

## Tutorial: Budget
We will now look at the budget info type, with `info_type_id = 105`.


```sql
%%sql
SELECT * 
FROM movie_info_sample
WHERE info_type_id = 105
ORDER BY id
LIMIT 10 OFFSET 5000;
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">10 rows affected.</span>





<table>
    <thead>
        <tr>
            <th>id</th>
            <th>movie_id</th>
            <th>info_type_id</th>
            <th>info</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1261074</td>
            <td>1983149</td>
            <td>105</td>
            <td>$75,000,000</td>
        </tr>
        <tr>
            <td>1261110</td>
            <td>1983269</td>
            <td>105</td>
            <td>INR 180,000,000</td>
        </tr>
        <tr>
            <td>1261160</td>
            <td>2381188</td>
            <td>105</td>
            <td>$40,000,000</td>
        </tr>
        <tr>
            <td>1261170</td>
            <td>1991083</td>
            <td>105</td>
            <td>FIM 9,219,499</td>
        </tr>
        <tr>
            <td>1261210</td>
            <td>1993907</td>
            <td>105</td>
            <td>$45,000,000</td>
        </tr>
        <tr>
            <td>1261247</td>
            <td>1995787</td>
            <td>105</td>
            <td>$38,000,000</td>
        </tr>
        <tr>
            <td>1261308</td>
            <td>1999081</td>
            <td>105</td>
            <td>$50,000,000</td>
        </tr>
        <tr>
            <td>1261324</td>
            <td>1999196</td>
            <td>105</td>
            <td>SEK 40,000,000</td>
        </tr>
        <tr>
            <td>1261375</td>
            <td>2001114</td>
            <td>105</td>
            <td>$60,000,000</td>
        </tr>
        <tr>
            <td>1261396</td>
            <td>2001989</td>
            <td>105</td>
            <td>$13,000,000</td>
        </tr>
    </tbody>
</table>
<span style="font-style:italic;text-align:center;">Truncated to <a href="https://jupysql.ploomber.io/en/latest/api/configuration.html#displaylimit">displaylimit</a> of 10.</span>



Similar to when we examined the gross info, we see a lot of non-US dollar currencies. For consistency, let's only use movies **with a budget in US dollars.**

## Question 3b:

Now, we want something similar for the budget of the film, so that we can perform the subtraction of `gross` and `budget`. We want the numerical part of the `info` column and the **maximum budget value** for a particular film (as you can verify, some movies have more than one budget). 

In the next cell,
- Construct a view named `movie_budget` containing one row for each movie, which has `(budget, movie_id, title)`, where `budget` is the numeric dollar amount extracted as a float.
- To take a look at our cleaned data, write a `SELECT` query to display the **top 10 highest budget films** from `movie_budget`. When multiple films have the same budget, break ties by `movie_id` (ascending).

**Hint:** The query here should be quite similar to Question 3a. Make sure to break ties properly!


```sql
%%sql --save query_3b result_3b <<

DROP VIEW IF EXISTS raw_budget CASCADE;
CREATE VIEW raw_budget AS(
    SELECT CAST(REGEXP_REPLACE(info, '[\D]', '', 'g') AS FLOAT) AS budget, movie_id, m.title AS title
    FROM movie_info_sample as i
    INNER JOIN movie_sample as m
        ON i.movie_id = m.id
    WHERE info_type_id = 105 
            AND info LIKE '%$%'
);
DROP VIEW IF EXISTS movie_budget;
CREATE VIEW movie_budget AS(
    SELECT MAX(budget) AS budget, movie_id, title
    FROM raw_budget
    GROUP BY movie_id, title
    ORDER BY movie_id ASC
);

SELECT *
FROM movie_budget
ORDER BY budget DESC
LIMIT 10;

```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">10 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_3b = %sqlcmd snippets query_3b
grading_util.save_results("result_3b", query_3b, result_3b)
result_3b
```




<table>
    <thead>
        <tr>
            <th>budget</th>
            <th>movie_id</th>
            <th>title</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>300000000.0</td>
            <td>2204343</td>
            <td>Pirates of the Caribbean: At World's End</td>
        </tr>
        <tr>
            <td>260000000.0</td>
            <td>2332419</td>
            <td>Tangled</td>
        </tr>
        <tr>
            <td>258000000.0</td>
            <td>2305993</td>
            <td>Spider-Man 3</td>
        </tr>
        <tr>
            <td>250000000.0</td>
            <td>2360588</td>
            <td>The Dark Knight Rises</td>
        </tr>
        <tr>
            <td>250000000.0</td>
            <td>2204347</td>
            <td>Pirates of the Caribbean: On Stranger Tides</td>
        </tr>
        <tr>
            <td>250000000.0</td>
            <td>2387922</td>
            <td>The Lone Ranger</td>
        </tr>
        <tr>
            <td>250000000.0</td>
            <td>1938937</td>
            <td>Harry Potter and the Half-Blood Prince</td>
        </tr>
        <tr>
            <td>250000000.0</td>
            <td>2002374</td>
            <td>John Carter</td>
        </tr>
        <tr>
            <td>237000000.0</td>
            <td>1704289</td>
            <td>Avatar</td>
        </tr>
        <tr>
            <td>230000000.0</td>
            <td>2344435</td>
            <td>The Amazing Spider-Man</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q3b")
```




<p><strong><pre style='display: inline;'>q3b</pre></strong> passed! 🌈</p>



<br><br>

---

## Question 3c

We have all the parts we need to calculate the profits. Using the `movie_gross` and `movie_budget` views created above, we can now subtract the numeric columns and save the result in another column called `profit`.

In the next cell, construct a view named `movie_profit` containing one row for each movie, which has `(movie_id, title, profit)`, where `profit` is the result of subtracting that movie's `budget` from `gross`. Following the view definition, write a `SELECT` query to return the **first 10 rows** of the view ordered by descending `profit`. This may take a while to execute.


```sql
%%sql --save query_3c result_3c <<

DROP VIEW IF EXISTS movie_profit;
CREATE VIEW movie_profit AS(
    SELECT g.movie_id AS movie_id, g.title AS title, (g.gross - b.budget) AS profit
    FROM movie_gross AS g
    JOIN movie_budget AS b ON g.movie_id = b.movie_id
);
SELECT * 
FROM movie_profit 
ORDER BY profit DESC
LIMIT 10;
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">10 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_3c = %sqlcmd snippets query_3c
grading_util.save_results("result_3c", query_3c, result_3c)
result_3c
```




<table>
    <thead>
        <tr>
            <th>movie_id</th>
            <th>title</th>
            <th>profit</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>1704289</td>
            <td>Avatar</td>
            <td>523507625.0</td>
        </tr>
        <tr>
            <td>2438179</td>
            <td>Titanic</td>
            <td>458672302.0</td>
        </tr>
        <tr>
            <td>2310522</td>
            <td>Star Wars</td>
            <td>449935665.0</td>
        </tr>
        <tr>
            <td>1851357</td>
            <td>E.T. the Extra-Terrestrial</td>
            <td>424610554.0</td>
        </tr>
        <tr>
            <td>2346436</td>
            <td>The Avengers</td>
            <td>403357910.0</td>
        </tr>
        <tr>
            <td>2360583</td>
            <td>The Dark Knight</td>
            <td>349858444.0</td>
        </tr>
        <tr>
            <td>2400712</td>
            <td>The Passion of the Christ</td>
            <td>340782930.0</td>
        </tr>
        <tr>
            <td>2006991</td>
            <td>Jurassic Park</td>
            <td>338820792.0</td>
        </tr>
        <tr>
            <td>2172509</td>
            <td>Olympus Has Fallen</td>
            <td>330824682.0</td>
        </tr>
        <tr>
            <td>2379293</td>
            <td>The Hunger Games</td>
            <td>330010692.0</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q3c")
```




<p><strong><pre style='display: inline;'>q3c</pre></strong> passed! 🙌</p>



<!-- BEGIN QUESTION -->

<br><br>

---

## Question 3d

We analyzed the data, but something seems odd. Upon closer look, there are many negative values for `profit`. For example, the movie `102 Dalmations` looks to have lost around $18M, but it was a widely successful film! What may account for this issue? Think about how we constrained our data from the start of the problem.

One of the reason behind this is we constrained our gross to only in the USA, so we will miss information about sells in dollars but not in the USA. 

<!-- END QUESTION -->

<br/><br/><br/>

<hr style="border: 1px solid #fdb515;" />

# Question 4: Using Cleaned Data

Now that we have cleaned our monetary records from the `info` attribute in `movie_info_sample`, let's take a closer look at the data we generated. 

<br><br>

---

## Question 4a: Earnings per Genre

Another `info_type` we can look at is the movie genre. Looking at the `movie_gross` values, how much does each *genre* earn on average in the US?

- Create a view with the columns `movie_id`, `title`, `gross`, `genre`, and `average_genre` where `gross` is a movie's gross US earnings, `genre` is the movie's genre, and `average_genre` is the average earnings for the corresponding genre. If a movie has multiple genres, the movie should appear in multiple rows with each genre as a row.

- Following the view definition, write a `SELECT` query to return the rows for the movie "Mr. & Mrs. Smith" ordered by genre alphabetically.

**Hint:** Look into [window functions](https://www.postgresql.org/docs/9.1/tutorial-window.html)



```sql
%%sql --save query_4a result_4a <<
DROP VIEW IF EXISTS movie_genre CASCADE;
CREATE VIEW movie_genre AS(
    SELECT movie_id, info AS genre
    FROM movie_info_sample
    WHERE info_type_id = 3
);

DROP VIEW IF EXISTS movie_avg_genre;
CREATE VIEW movie_avg_genre AS(
    SELECT m.movie_id AS movie_id, title, gross, genre,
            AVG(gross) OVER (PARTITION BY genre) AS average_genre
    FROM movie_gross as m
    INNER JOIN movie_genre as g
    ON m.movie_id = g.movie_id
    
);

SELECT * FROM movie_avg_genre
WHERE title ='Mr. & Mrs. Smith'
ORDER BY genre;
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">3 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_4a = %sqlcmd snippets query_4a
grading_util.save_results("result_4a", query_4a, result_4a)
result_4a
```




<table>
    <thead>
        <tr>
            <th>movie_id</th>
            <th>title</th>
            <th>gross</th>
            <th>genre</th>
            <th>average_genre</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>2132092</td>
            <td>Mr. & Mrs. Smith</td>
            <td>186336103.0</td>
            <td>Action</td>
            <td>42123826.131625965</td>
        </tr>
        <tr>
            <td>2132092</td>
            <td>Mr. & Mrs. Smith</td>
            <td>186336103.0</td>
            <td>Comedy</td>
            <td>21583843.81801513</td>
        </tr>
        <tr>
            <td>2132092</td>
            <td>Mr. & Mrs. Smith</td>
            <td>186336103.0</td>
            <td>Romance</td>
            <td>18470817.081399772</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q4a")
```




<p><strong><pre style='display: inline;'>q4a</pre></strong> passed! 🙌</p>



<br><br>

---

## Question 4b: Analyzing Gross Earnings

A common way to view numerical data is with a boxplot. A boxplot shows a spread of the data along with several other key attributes that allow for further data analysis. 

We went through a lot of work transforming the gross earnings from strings in the`info` attribute into a numerical value. Because of our hard work, we can now further examine this data and understand its distribution. To do this, we first need to generate a [five-number summary](https://en.wikipedia.org/wiki/Five-number_summary) and find the average of the US gross earnings data.

- Create a view named `earnings_summary`, which consists of a one row summary of the `movie_gross` `gross` data with the `min`, `25th_percentile`, `median`, `75th_percentile`, `max`, and `average`. 
- Following the view definition, write a `SELECT` query to display it.

**Hint:** Look at SQL [aggregate functions](https://www.postgresql.org/docs/9.4/functions-aggregate.html). You may find some useful.


```sql
%%sql --save query_4b result_4b <<

DROP VIEW IF EXISTS earnings_summary;
CREATE VIEW earnings_summary AS(
    SELECT PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY gross) AS min, 
        PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY gross) AS "25th_percentile",
        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY gross) AS median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY gross) AS "75th_percentile",
        PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY gross) AS max,
        AVG(gross) AS average
    FROM movie_gross
    LIMIT 1
);
SELECT * 
FROM earnings_summary
LIMIT 2;
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">1 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_4b = %sqlcmd snippets query_4b
grading_util.save_results("result_4b", query_4b, result_4b)
result_4b
```




<table>
    <thead>
        <tr>
            <th>min</th>
            <th>25th_percentile</th>
            <th>median</th>
            <th>75th_percentile</th>
            <th>max</th>
            <th>average</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>30.0</td>
            <td>166623.0</td>
            <td>2317091.0</td>
            <td>20002717.5</td>
            <td>760507625.0</td>
            <td>19594424.63641884</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q4b")
```




<p><strong><pre style='display: inline;'>q4b</pre></strong> passed! 🍀</p>



<!-- BEGIN QUESTION -->

<br><br>

---

## Question 4c
What do you notice about the summary values generated in `earnings_summary`? We can represent the five-number summary graphically using a [box plot](https://en.wikipedia.org/wiki/Box_plot). Identify two properties about the boxplot of the data. (You do not need to explicitly create a boxplot, but think about how the summary statistics would be distributed in a boxplot.)

**Hint:** Think in terms of about concepts from statistics like spread, modality, skew, etc. and how they may apply here.

The first property about the boxplot of the data is that this is heavily skewed to the right with most of the data concentrate around small value and few data with big value. This can be justified by looking at the mean (19 millions) is significantly higher than the median(2 millions). Another property is this boxplot has a large spread with min equals 30 and max is around 760 millions.


```python
# %%sql --save query_4c<<
# SELECT * FROM movie_gross;
```


```python
# query_4c = %sqlcmd snippets query_4c
```


```python
# optional: include your plotting code here
# import pandas as pd
# import seaborn as sns
# import matplotlib.pyplot as plt

# gross = pd.read_sql(query_4c, 'postgresql://jovyan@127.0.0.1:5432/imdb')
# sns.boxplot(x=gross['gross'])
# plt.axvline(x=np.mean(gross['gross']), color='r')
# plt.title('Boxplot for Gross')
# plt.show()

```

<!-- END QUESTION -->

<br/><br/><br/>

<hr style="border: 1px solid #fdb515;" />

# Question 5: Joins

Joins are a powerful tool in database cleaning and analysis. They allow for the user to create useful tables and bring together information in a meaningful way. 

There are many types of joins: inner, outer, left, right, etc. Let's practice these in a special scenario. 

You are now working as a talent director and you need a list of all people who have been in `actor` roles and the number of movies in which they have acted. 

- Create a view called `number_movies`, which has columns `id`, `name`, `number` where `id` is the actor's id, `name` is the actor's name, and `number` is the number of movies they have acted in.
- Following your view, write a ``SELECT`` query to display the **top 10 actors** who have been in the most films.

**Note:** The `cast_sample` may include actors not included in `actor_sample` table. We still want to include these actors in our result by reference to their id. The `name` field can be NULL.


```sql
%%sql --save query_5 result_5 <<

DROP VIEW IF EXISTS number_movies;
CREATE VIEW number_movies AS (
    SELECT DISTINCT c.person_id AS id, a.name AS name, COUNT(movie_id) OVER(PARTITION BY c.person_id) AS number
    FROM cast_sample AS c
    LEFT JOIN actor_sample AS a
        ON c.person_id = a.id
    WHERE c.role_id = 1
);
SELECT *
FROM number_movies
ORDER BY number DESC
LIMIT 10;
```


<span style="None">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>



<span style="color: green">10 rows affected.</span>



```python
# Do not delete/edit this cell!
# You must run this cell before running the autograder.
query_5 = %sqlcmd snippets query_5
grading_util.save_results("result_5", query_5, result_5)
result_5
```




<table>
    <thead>
        <tr>
            <th>id</th>
            <th>name</th>
            <th>number</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>95397</td>
            <td>Barker, Bob</td>
            <td>6853</td>
        </tr>
        <tr>
            <td>515315</td>
            <td>Freeman, Morgan</td>
            <td>5938</td>
        </tr>
        <tr>
            <td>677696</td>
            <td>Hinnant, Skip</td>
            <td>4697</td>
        </tr>
        <tr>
            <td>1573853</td>
            <td>Trebek, Alex</td>
            <td>4690</td>
        </tr>
        <tr>
            <td>1362169</td>
            <td>Sajak, Pat</td>
            <td>3937</td>
        </tr>
        <tr>
            <td>1417394</td>
            <td>Shaffer, Paul</td>
            <td>3546</td>
        </tr>
        <tr>
            <td>911160</td>
            <td>Lima, Pedro</td>
            <td>2911</td>
        </tr>
        <tr>
            <td>900749</td>
            <td>Letterman, David</td>
            <td>2895</td>
        </tr>
        <tr>
            <td>487253</td>
            <td>Filipe, Guilherme</td>
            <td>2861</td>
        </tr>
        <tr>
            <td>356575</td>
            <td>Davidson, Doug</td>
            <td>2760</td>
        </tr>
    </tbody>
</table>




```python
grader.check("q5")
```




<p><strong><pre style='display: inline;'>q5</pre></strong> passed! 💯</p>



<hr style="border: 5px solid #003262;" />
<hr style="border: 1px solid #fdb515;" />

# Congratulations! You have finished Project 1.

The below code prepares all the additional files needed for your submission, including:
* `results.zip`
* `proj1.pdf`

**Make sure to run this cell before exporting the final zip file with `grader.export()`!**


```python
grading_util.prepare_submission_and_cleanup() 
!otter export -e latex proj1.ipynb proj1.pdf   # Export PDF
```

    /srv/conda/envs/notebook/lib/python3.11/site-packages/nbconvert/utils/pandoc.py:51: RuntimeWarning: You are using an unsupported version of pandoc (2.12).
    Your version must be at least (2.14.2) but less than (4.0.0).
    Refer to https://pandoc.org/installing.html.
    Continuing with doubts...
      check_pandoc_version()



```python
# Close SQL magic connection
%sql --close postgresql://127.0.0.1:5432/imdb
```

    RuntimeError: Could not close connection because it was not found amongst these: ['postgresql://jovyan@127.0.0.1:5432/imdb']
    If you need help solving this issue, send us a message: https://ploomber.io/community


## Submission

Make sure you have run all cells in your notebook in order before running the cell below, so that all images/graphs appear in the output. The cell below will generate a zip file for you to submit. **Please save before exporting!**

After you have run the cell below and generated the zip file, you can download your PDF <a href='hw01.pdf' download>here</a>.


```python
# Save your notebook first, then run this cell to export your submission.
grader.export(pdf=False, run_tests=True, files=['proj1.pdf', 'results.zip'])
```

    Running your submission against local test cases...
    
    
    
    Your submission received the following results when run against available test cases:
    
        q0 results: All test cases passed!
    
        q1a results: All test cases passed!
    
        q1b results: All test cases passed!
    
        q1c results: All test cases passed!
    
        q1d results: All test cases passed!
    
        q2a results: All test cases passed!
    
        q2b results: All test cases passed!
    
        q2c results: All test cases passed!
    
        q3a results: All test cases passed!
    
        q3b results: All test cases passed!
    
        q3c results: All test cases passed!
    
        q4a results: All test cases passed!
    
        q4b results: All test cases passed!
    
        q5 results: All test cases passed!




<p>
    Your submission has been exported. Click
    <a href="proj1_2023_09_14T19_54_44_927071.zip" download="proj1_2023_09_14T19_54_44_927071.zip" target="_blank">here</a> to download
    the zip file.
</p>



 
