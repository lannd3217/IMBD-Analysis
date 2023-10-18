{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 244,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [],
   "source": [
    "# Initialize Otter\n",
    "import otter\n",
    "grader = otter.Notebook(\"proj1.ipynb\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Project 1 - SQL\n",
    "## Due Date: Thursday, September 21st, 5:00pm\n",
    "\n",
    "In this project, we will be working with SQL on the IMDB database.\n",
    "\n",
    "## Objectives\n",
    "\n",
    "- Explore and extract relevant information from database with SQL functions\n",
    "- Perform data cleaning and transformation using string functions and regex\n",
    "- Use the cleaned data to run insightful analysis using joins, aggregations, and window functions\n",
    "\n",
    "**Note:** If at any point during the project, the internal state of the database or its tables have been modified in an undesirable way (i.e. a modification not resulting from the instructions of a question), restart your kernel, clear output, and simply re-run the notebook as normal. This will shutdown your current connection to the database, which will prevent the issue of multiple connections to the database at any given point. When re-running the notebook, you will create a fresh database based on the provided Postgres dump."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Logistics & Scoring Breakdown\n",
    "\n",
    "Each coding question has **both public tests and hidden tests**. Roughly 50% of your grade will be made up of your score on the public tests released to you, while the remaining 50% will be made up of unreleased hidden tests. In addition, there are two free-response questions that will be manually graded.\n",
    "\n",
    "This is an **individual project**. However, you’re welcome to collaborate with any other student in the class as long as it’s within the [academic honesty guidelines](https://fa23.data101.org/syllabus/#collaboration-and-integrity). Create new cells as needed to acknowledge others.\n",
    "\n",
    "|Question|Points|\n",
    "|---|---|\n",
    "|0|1|\n",
    "|1a|1|\n",
    "|1b|2|\n",
    "|1c|1|\n",
    "|1d|1|\n",
    "|2a|1|\n",
    "|2b|3|\n",
    "|2c|3|\n",
    "|3a|2|\n",
    "|3b|2|\n",
    "|3c|2|\n",
    "|3d|1|\n",
    "|4a|2|\n",
    "|4b|2|\n",
    "|4c|1|\n",
    "|5|2|\n",
    "|**Total**|27|"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 245,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Run this cell to set up imports\n",
    "import numpy as np\n",
    "import pandas as pd"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<hr style=\"border: 5px solid #003262;\" />\n",
    "<hr style=\"border: 1px solid #fdb515;\" />"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Before You Start: Assignment Tips\n",
    "\n",
    "<div class=\"alert alert-block alert-info\">\n",
    "<p><b>Please Read!!</b> In this project we will assume you have attended lecture and seen how to connect to a Postgres server via two ways: JupySQL in Jupyter Notebook, and the psql command-line program.</p>\n",
    "</div>\n",
    "    \n",
    "We have written up these instructions for you in the <a href=\"https://fa23.data101.org/resources/assignment-tips/\">Fall 2023 Assignment Tips</a>—a handy resource that has many other tips:\n",
    "\n",
    "* PostgreSQL documentation\n",
    "* JupySQL and magic commands in Jupyter\n",
    "* JupyterHub keyboard shortcuts\n",
    "* psql and common meta-commands\n",
    "* Debugging:\n",
    "    * Where to create new cells to play nice with the autograder\n",
    "    * Opening/closing connections, deleting databases if all else fails\n",
    "* Local installation (not supported by staff officially, but for your reference)\n",
    "\n",
    "\n",
    "\n",
    "For some questions with multi-line cell magic, we will also be saving the literal query string with [query snippets](https://jupysql.ploomber.io/en/latest/api/magic-snippets.html) using `--save`:\n",
    "\n",
    "``%%sql --save query result << select * FROM table ...``"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Database Setup\n",
    "We are going to be using the `JupySQL` library to connect our notebook to a PostgreSQL database server on your JupyterHub account. Running the next cell will do so; you should not see any error messages after it executes."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 246,
   "metadata": {},
   "outputs": [],
   "source": [
    "# The first time you are running this cell, you may need to run the following line as: %load_ext sql \n",
    "%reload_ext sql"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "In the next cell, we will unzip the data. This only needs to be done once."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 247,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Archive:  data/imdbdb.zip\n"
     ]
    }
   ],
   "source": [
    "!unzip -u data/imdbdb.zip -d data/"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<br/>\n",
    "\n",
    "**Create the `imdb` database**: <br>\n",
    "We will use PostgreSQL commands to create a database and import our data into it. Run the following cell to do this.\n",
    "* You can also run these cells in the command-line via `psql`.\n",
    "* If you run into the **role does not exist** error, feel free to ignore it. It does not affect data import."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 248,
   "metadata": {},
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      " pg_terminate_backend \n",
      "----------------------\n",
      " t\n",
      "(1 row)\n",
      "\n",
      "DROP DATABASE\n",
      "CREATE DATABASE\n",
      "SET\n",
      "SET\n",
      "SET\n",
      "SET\n",
      "SET\n",
      " set_config \n",
      "------------\n",
      " \n",
      "(1 row)\n",
      "\n",
      "SET\n",
      "SET\n",
      "SET\n",
      "SET\n",
      "SET\n",
      "SET\n",
      "CREATE TABLE\n",
      "ALTER TABLE\n",
      "CREATE TABLE\n",
      "ALTER TABLE\n",
      "CREATE TABLE\n",
      "ALTER TABLE\n",
      "CREATE TABLE\n",
      "ALTER TABLE\n",
      "CREATE TABLE\n",
      "ALTER TABLE\n",
      "CREATE TABLE\n",
      "ALTER TABLE\n",
      "COPY 500000\n",
      "COPY 3804162\n",
      "COPY 113\n",
      "COPY 2433431\n",
      "COPY 337179\n",
      "COPY 12\n",
      "ALTER TABLE\n",
      "ALTER TABLE\n"
     ]
    }
   ],
   "source": [
    "!psql postgresql://jovyan@127.0.0.1:5432/imdb -c 'SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = current_database()  AND pid <> pg_backend_pid();'\n",
    "!psql postgresql://jovyan@127.0.0.1:5432/postgres -c 'DROP DATABASE IF EXISTS imdb'\n",
    "!psql postgresql://jovyan@127.0.0.1:5432/postgres -c 'CREATE DATABASE imdb'\n",
    "!psql postgresql://jovyan@127.0.0.1:5432/imdb -f data/imdbdb.sql"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**Connect to `imdb` database in the Notebook**: \n",
    "<br>\n",
    "Now let's connect to the new database we just created! There should be no errors after running the following cell."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 249,
   "metadata": {},
   "outputs": [],
   "source": [
    "%sql postgresql://jovyan@127.0.0.1:5432/imdb"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**Connect to `imdb` database in `psql`**: \n",
    "\n",
    "<div class=\"alert alert-block alert-info\">\n",
    "<b>Do the following in a Terminal window!</b>\n",
    "</div>\n",
    "\n",
    "Connect to the same database via `psql`. See the [Fall 2023 Assignment Tips](https://fa23.data101.org/resources/assignment-tips/) website resource for details on connecting. Run the following meta-command in the `psql` client:\n",
    "\n",
    "``\n",
    "\\l\n",
    "``\n",
    "\n",
    "This should display all databases on this server, including the `imdb` database you just created.\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "---\n",
    "\n",
    "**Quick check**: To make sure things are working, let's fetch 10 rows from one of our tables `cast_sample`. Just run the following cell, no further action is needed."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 250,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "/srv/conda/envs/notebook/lib/python3.11/site-packages/sql/connection/connection.py:827: JupySQLRollbackPerformed: Server closed connection. JupySQL executed a ROLLBACK operation.\n",
      "  warnings.warn(\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">10 rows affected.</span>"
      ],
      "text/plain": [
       "10 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>id</th>\n",
       "            <th>person_id</th>\n",
       "            <th>movie_id</th>\n",
       "            <th>role_id</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>708</td>\n",
       "            <td>235</td>\n",
       "            <td>2345369</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>721</td>\n",
       "            <td>241</td>\n",
       "            <td>2504309</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>789</td>\n",
       "            <td>264</td>\n",
       "            <td>2156734</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>875</td>\n",
       "            <td>299</td>\n",
       "            <td>1954994</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>888</td>\n",
       "            <td>302</td>\n",
       "            <td>765037</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>889</td>\n",
       "            <td>302</td>\n",
       "            <td>765172</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>898</td>\n",
       "            <td>306</td>\n",
       "            <td>291387</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>899</td>\n",
       "            <td>306</td>\n",
       "            <td>1477434</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>931</td>\n",
       "            <td>324</td>\n",
       "            <td>824119</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1936</td>\n",
       "            <td>543</td>\n",
       "            <td>1754068</td>\n",
       "            <td>1</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>\n",
       "<span style=\"font-style:italic;text-align:center;\">Truncated to <a href=\"https://jupysql.ploomber.io/en/latest/api/configuration.html#displaylimit\">displaylimit</a> of 10.</span>"
      ],
      "text/plain": [
       "+------+-----------+----------+---------+\n",
       "|  id  | person_id | movie_id | role_id |\n",
       "+------+-----------+----------+---------+\n",
       "| 708  |    235    | 2345369  |    1    |\n",
       "| 721  |    241    | 2504309  |    1    |\n",
       "| 789  |    264    | 2156734  |    1    |\n",
       "| 875  |    299    | 1954994  |    1    |\n",
       "| 888  |    302    |  765037  |    1    |\n",
       "| 889  |    302    |  765172  |    1    |\n",
       "| 898  |    306    |  291387  |    1    |\n",
       "| 899  |    306    | 1477434  |    1    |\n",
       "| 931  |    324    |  824119  |    1    |\n",
       "| 1936 |    543    | 1754068  |    1    |\n",
       "+------+-----------+----------+---------+\n",
       "Truncated to displaylimit of 10."
      ]
     },
     "execution_count": 250,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "%%sql\n",
    "SELECT * \n",
    "  FROM cast_sample\n",
    "LIMIT 10"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "## Connect to the grader"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 251,
   "metadata": {},
   "outputs": [],
   "source": [
    "# Connecting the grader\n",
    "# Just run the following cell, no further action is needed.\n",
    "from data101_utils import GradingUtil\n",
    "grading_util = GradingUtil(\"proj1\")\n",
    "grading_util.prepare_autograder()"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "jp-MarkdownHeadingCollapsed": true
   },
   "source": [
    "## The `imdb` Database\n",
    "\n",
    "In this project, we are working with a reduced version of the Internet Movie Database (IMDb) database. This Postgres database is a small random sample of actors from the much larger full database (which is over several GBs large) and includes their corresponding movies and cast info. Disclaimer: as a result, we may obtain wildly  different results than if we were to use the entire database. \n",
    "\n",
    "- **actor_sample**: information about the actors including id, name, and gender\n",
    "- **cast_sample**: each person on the cast of each movie gets a row including cast id, each person's id (`actor_sample.id`), movie id (`movie_sample.id`), and role id\n",
    "- **movie_sample**: sample of movies the actors have been in, including movie id, title, and the production year\n",
    "- **movie_info_sample**: this table originally had a lot of information for each movie (take a look at info_type to see the information available), but we have dropped some information to make it easier to manage. This table includes movie info's id, movie id, info type id, and the info itself\n",
    "- **info_type**: reference table to match each info type id to the description of the type of information\n",
    "- **role_type**: reference table for cast_sample to match role id to the description of the role\n",
    "\n",
    "### Key Notes\n",
    "- This database is **not** the same as the IMDb lecture database, but has a lot of of similar features. \n",
    "- Point of confusion: `movie_sample` and `actor_sample` both have attributes `id` corresponding to 7 digit unique numeric identifiers, but do **not** refer to the same data values.\n",
    "- `cast_sample` is analagous to the `crew` table from lecture. It can be used to match an actor's id to movies they have acted in, among other relations.\n",
    "- You are highly encouraged to spend some time exploring the metadata of these tables using Postgres meta-commands to better understand the data given and the relations between tables."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<br/><br/><br/>\n",
    "\n",
    "<hr style=\"border: 1px solid #fdb515;\" />\n",
    "\n",
    "# The `information_schema` schema"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "A **schema** is a namespace of tables in the database, often used for security purposes. Let's see how many schema are defined for us in our current database:\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 252,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">4 rows affected.</span>"
      ],
      "text/plain": [
       "4 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>catalog_name</th>\n",
       "            <th>schema_name</th>\n",
       "            <th>schema_owner</th>\n",
       "            <th>default_character_set_catalog</th>\n",
       "            <th>default_character_set_schema</th>\n",
       "            <th>default_character_set_name</th>\n",
       "            <th>sql_path</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>pg_toast</td>\n",
       "            <td>jovyan</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>pg_catalog</td>\n",
       "            <td>jovyan</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>public</td>\n",
       "            <td>jovyan</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>information_schema</td>\n",
       "            <td>jovyan</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+--------------+--------------------+--------------+-------------------------------+------------------------------+----------------------------+----------+\n",
       "| catalog_name |    schema_name     | schema_owner | default_character_set_catalog | default_character_set_schema | default_character_set_name | sql_path |\n",
       "+--------------+--------------------+--------------+-------------------------------+------------------------------+----------------------------+----------+\n",
       "|     imdb     |      pg_toast      |    jovyan    |              None             |             None             |            None            |   None   |\n",
       "|     imdb     |     pg_catalog     |    jovyan    |              None             |             None             |            None            |   None   |\n",
       "|     imdb     |       public       |    jovyan    |              None             |             None             |            None            |   None   |\n",
       "|     imdb     | information_schema |    jovyan    |              None             |             None             |            None            |   None   |\n",
       "+--------------+--------------------+--------------+-------------------------------+------------------------------+----------------------------+----------+"
      ]
     },
     "execution_count": 252,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "%%sql\n",
    "SELECT * \n",
    "FROM information_schema.schemata;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Within a Postgres database, there are often at least three schemas:\n",
    "* `public`, a public schema that users can access and create tables in;\n",
    "* `pg_catalog`, a schema for maintaining system information; and\n",
    "* `information_schema`, a schema that maintains metadata about objects currently created in the database.\n",
    "* The fourth schema `pg_toast` maintains data that can't regularly be stored in relations, such as very large data values. See more in documentation [here](https://www.postgresql.org/docs/current/storage-toast.html).\n",
    "\n",
    "For now, we focus on the `information_schema` schemata, which stores our metadata. That’s right—metadata is also data, and as we make updates to our `public` databases, metadata is automatically stored and updated into different tables under the `information_schema` schema.\n",
    "\n",
    "There are many metadata tables that Postgres updates for us, and the full list is in the Postgres documentation [(Chapter 37)](https://www.postgresql.org/docs/current/information-schema.html). \n",
    "For now, let’s look at which the `.tables` table ([37.54](https://www.postgresql.org/docs/current/infoschema-tables.html)), which lists all the tables located in the database. Let’s specifically look at those that are in the public schema (i.e., publicly accessible tables):"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 253,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">6 rows affected.</span>"
      ],
      "text/plain": [
       "6 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>table_catalog</th>\n",
       "            <th>table_schema</th>\n",
       "            <th>table_name</th>\n",
       "            <th>table_type</th>\n",
       "            <th>self_referencing_column_name</th>\n",
       "            <th>reference_generation</th>\n",
       "            <th>user_defined_type_catalog</th>\n",
       "            <th>user_defined_type_schema</th>\n",
       "            <th>user_defined_type_name</th>\n",
       "            <th>is_insertable_into</th>\n",
       "            <th>is_typed</th>\n",
       "            <th>commit_action</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>public</td>\n",
       "            <td>actor_sample</td>\n",
       "            <td>BASE TABLE</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>YES</td>\n",
       "            <td>NO</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>public</td>\n",
       "            <td>cast_sample</td>\n",
       "            <td>BASE TABLE</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>YES</td>\n",
       "            <td>NO</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>public</td>\n",
       "            <td>info_type</td>\n",
       "            <td>BASE TABLE</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>YES</td>\n",
       "            <td>NO</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>public</td>\n",
       "            <td>movie_info_sample</td>\n",
       "            <td>BASE TABLE</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>YES</td>\n",
       "            <td>NO</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>public</td>\n",
       "            <td>movie_sample</td>\n",
       "            <td>BASE TABLE</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>YES</td>\n",
       "            <td>NO</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>imdb</td>\n",
       "            <td>public</td>\n",
       "            <td>role_type</td>\n",
       "            <td>BASE TABLE</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>None</td>\n",
       "            <td>YES</td>\n",
       "            <td>NO</td>\n",
       "            <td>None</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+---------------+--------------+-------------------+------------+------------------------------+----------------------+---------------------------+--------------------------+------------------------+--------------------+----------+---------------+\n",
       "| table_catalog | table_schema |     table_name    | table_type | self_referencing_column_name | reference_generation | user_defined_type_catalog | user_defined_type_schema | user_defined_type_name | is_insertable_into | is_typed | commit_action |\n",
       "+---------------+--------------+-------------------+------------+------------------------------+----------------------+---------------------------+--------------------------+------------------------+--------------------+----------+---------------+\n",
       "|      imdb     |    public    |    actor_sample   | BASE TABLE |             None             |         None         |            None           |           None           |          None          |        YES         |    NO    |      None     |\n",
       "|      imdb     |    public    |    cast_sample    | BASE TABLE |             None             |         None         |            None           |           None           |          None          |        YES         |    NO    |      None     |\n",
       "|      imdb     |    public    |     info_type     | BASE TABLE |             None             |         None         |            None           |           None           |          None          |        YES         |    NO    |      None     |\n",
       "|      imdb     |    public    | movie_info_sample | BASE TABLE |             None             |         None         |            None           |           None           |          None          |        YES         |    NO    |      None     |\n",
       "|      imdb     |    public    |    movie_sample   | BASE TABLE |             None             |         None         |            None           |           None           |          None          |        YES         |    NO    |      None     |\n",
       "|      imdb     |    public    |     role_type     | BASE TABLE |             None             |         None         |            None           |           None           |          None          |        YES         |    NO    |      None     |\n",
       "+---------------+--------------+-------------------+------------+------------------------------+----------------------+---------------------------+--------------------------+------------------------+--------------------+----------+---------------+"
      ]
     },
     "execution_count": 253,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "%%sql\n",
    "SELECT * \n",
    "FROM information_schema.tables\n",
    "WHERE table_schema = 'public';"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br/><br/><br/>\n",
    "\n",
    "<hr style=\"border: 1px solid #fdb515;\" />\n",
    "\n",
    "\n",
    "# Question 0\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "As stated above, there are many metadata tables stored in the `information_schema` schema. Write a query that returns the names of all relations in the PostgreSQL `information_schema` schema, i.e., the names of all the metadata tables\n",
    "\n",
    "**Hints:**\n",
    "* Your resulting table names should correspond to what’s listed in the information schema documentation [(Chapter 37)](https://www.postgresql.org/docs/15/information-schema.html). \n",
    "* For you to think about: Why might there be fewer tables in your query response than the full list in the documentation? "
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 254,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">69 rows affected.</span>"
      ],
      "text/plain": [
       "69 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_0 result_0 <<\n",
    "SELECT table_name \n",
    "FROM information_schema.tables\n",
    "WHERE table_schema = 'information_schema';"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 255,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>table_name</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>information_schema_catalog_name</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>attributes</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>applicable_roles</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>administrable_role_authorizations</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>check_constraint_routine_usage</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>character_sets</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>check_constraints</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>collations</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>collation_character_set_applicability</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>column_column_usage</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+---------------------------------------+\n",
       "|               table_name              |\n",
       "+---------------------------------------+\n",
       "|    information_schema_catalog_name    |\n",
       "|               attributes              |\n",
       "|            applicable_roles           |\n",
       "|   administrable_role_authorizations   |\n",
       "|     check_constraint_routine_usage    |\n",
       "|             character_sets            |\n",
       "|           check_constraints           |\n",
       "|               collations              |\n",
       "| collation_character_set_applicability |\n",
       "|          column_column_usage          |\n",
       "+---------------------------------------+"
      ]
     },
     "execution_count": 255,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_0 = %sqlcmd snippets query_0\n",
    "grading_util.save_results(\"result_0\", query_0, result_0)\n",
    "result_0"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 256,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q0</pre></strong> passed! 🍀</p>"
      ],
      "text/plain": [
       "q0 results: All test cases passed!"
      ]
     },
     "execution_count": 256,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q0\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<br/><br/><br/>\n",
    "\n",
    "<hr style=\"border: 1px solid #fdb515;\" />\n",
    "\n",
    "\n",
    "# Question 1: Exploratory Data Analysis\n",
    "One of the first things you'll want to do with a database table is get a sense for its metadata: column names and types, and number of rows. \n",
    "\n",
    "## Tutorial \n",
    "\n",
    "We can use the PostgreSQL `\\d` meta-command to get a description of all the columns in the `movie_info_sample` table. Open up a terminal window, connect to the imdb server, and analyze the output of the meta-command:"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "``\\d movie_info_sample``"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "We can use the PostgreSQL `\\d` meta-command to get a description the `movie_info_sample` schema. Open up a terminal window, connect to the imdb server, and analyze the output of the meta-command:\n",
    "\n",
    "``\\d movie_info_sample``\n",
    "\n",
    "There are four attributes in this schema, of which `\"id\"` is one. What are the other attribute names? Assign `result_1a` to a list of strings, where each element is an attribute name. The list does not need to be in order."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 257,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/plain": [
       "['id', 'movie_id', 'info_type_id', 'info']"
      ]
     },
     "execution_count": 257,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "result_1a = [\"id\", \"movie_id\", \"info_type_id\", \"info\"]\n",
    "result_1a"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 258,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q1a</pre></strong> passed! 🌈</p>"
      ],
      "text/plain": [
       "q1a results: All test cases passed!"
      ]
     },
     "execution_count": 258,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q1a\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "**Debugging tip**: Throughout this project and when working with databases, you should always be checking schemas via the `\\d` psql metacommand."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 1b"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "Next, let’s continue with our initial exploration of this table. How many rows are in this table? \n",
    "\n",
    "Assign `result_1b` to the result of a SQL query to calculate the number of rows in the `movie_info_sample` table. Then, assign `count_1b` to the integer number of rows based on what you found in `result_1b`. Do not hard code this value.\n",
    "\n",
    "**Hints:**\n",
    "- See the [Assignment Tips](https://fa23.data101.org/resources/assignment-tips/) page for how to use SQL line magic.\n",
    "- Your query result should have exactly one row and one attribute; the lone value in the instance should be the number of rows.\n",
    "- See the `JupySQL` [documentation](https://jupysql.ploomber.io/en/latest/intro.html) for how to index into a SQL query result."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 259,
   "metadata": {
    "scrolled": true,
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">1 rows affected.</span>"
      ],
      "text/plain": [
       "1 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>count</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>2433431</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+---------+\n",
       "|  count  |\n",
       "+---------+\n",
       "| 2433431 |\n",
       "+---------+"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/plain": [
       "2433431"
      ]
     },
     "execution_count": 259,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "result_1b = %sql SELECT COUNT(*) FROM movie_info_sample; \n",
    "# replace with %sql command\n",
    "count_1b = result_1b[0][0]\n",
    "\n",
    "# do not edit below this line\n",
    "display(result_1b)\n",
    "count_1b"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 260,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q1b</pre></strong> passed! 🍀</p>"
      ],
      "text/plain": [
       "q1b results: All test cases passed!"
      ]
     },
     "execution_count": 260,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q1b\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 1c: Random table sample\n",
    "\n",
    "Now that we know a bit about the metadata of the table, let's randomly sample rows from `movie_info_sample` to explore its contents.\n",
    "\n",
    "Given that you know the size of the table from the previous query, **write a query that retrieves 5 tuples on expectation using the `BERNOULLI` sampling method.** That is, if we run the query multiple times, we should get 5 tuples on average in our resulting table. The `BERNOULLI` sampling method scans the whole table and selects individual rows independently with `p%` probability. Please see the [documentation](https://www.postgresql.org/docs/15/sql-select.html) for syntax.\n",
    "\n",
    "\n",
    "**Hints/Details:**\n",
    "* Assign `p_1c` to a sampling rate that you pass into the `query_1c` f-string using Python variable substitution. Your formula should contain `count_1b`. Don't forget to express `p_1c` in units of percent, i.e., `p_1c = 0.03` is 0.03%!\n",
    "* For a refresher on f-strings and Python variable substitution, see [this tutorial](https://www.geeksforgeeks.org/formatted-string-literals-f-strings-python/). If Python variable substitution is done correctly, we should be able to change our `p%` probability by simply reassigning `p_1c` and rerunning the query. (Please leave `p_1c` unchanged.)\n",
    "* We have completed the SQL line magic for you; this references the Python f-string `query_1c` you created within a SQL query using JupySQL-specific syntax.\n",
    "* Try running the SQL cell many times and see what you notice."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 261,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">2 rows affected.</span>"
      ],
      "text/plain": [
       "2 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "p_1c = 5/count_1b*100\n",
    "query_1c = f\"SELECT * FROM movie_info_sample TABLESAMPLE BERNOULLI({p_1c});\" \n",
    "# edit this query string\n",
    "\n",
    "# Do not edit below this line\n",
    "result_1c = %sql {{query_1c}}"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 262,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>id</th>\n",
       "            <th>movie_id</th>\n",
       "            <th>info_type_id</th>\n",
       "            <th>info</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>4398443</td>\n",
       "            <td>1024138</td>\n",
       "            <td>8</td>\n",
       "            <td>France</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1474910</td>\n",
       "            <td>1999078</td>\n",
       "            <td>105</td>\n",
       "            <td>$350,000</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+---------+----------+--------------+----------+\n",
       "|    id   | movie_id | info_type_id |   info   |\n",
       "+---------+----------+--------------+----------+\n",
       "| 4398443 | 1024138  |      8       |  France  |\n",
       "| 1474910 | 1999078  |     105      | $350,000 |\n",
       "+---------+----------+--------------+----------+"
      ]
     },
     "execution_count": 262,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "grading_util.save_results(\"result_1c\", query_1c, result_1c)\n",
    "result_1c"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 263,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q1c</pre></strong> passed! ✨</p>"
      ],
      "text/plain": [
       "q1c results: All test cases passed!"
      ]
     },
     "execution_count": 263,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q1c\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 1d: Random sample, fixed number of rows\n",
    "\n",
    "If a random number of rows is not of importance, a more efficient way to get some arbitrary tuples from a table is to use the `ORDER BY` and `LIMIT` clauses. In the next cell, fetch 5 **random** tuples from `movie_info_sample`. Compared to the previous question, your query result here should always have 5 tuples!\n",
    "\n",
    "**Hint**: Check out lecture.\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 264,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">5 rows affected.</span>"
      ],
      "text/plain": [
       "5 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_1d result_1d <<\n",
    "SELECT * FROM movie_info_sample ORDER BY RANDOM() LIMIT 5;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 265,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>id</th>\n",
       "            <th>movie_id</th>\n",
       "            <th>info_type_id</th>\n",
       "            <th>info</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>1631874</td>\n",
       "            <td>1102704</td>\n",
       "            <td>105</td>\n",
       "            <td>$250,000</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>9722642</td>\n",
       "            <td>2434417</td>\n",
       "            <td>1</td>\n",
       "            <td>60</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>9503842</td>\n",
       "            <td>2098373</td>\n",
       "            <td>1</td>\n",
       "            <td>7</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>4903218</td>\n",
       "            <td>1974739</td>\n",
       "            <td>8</td>\n",
       "            <td>Italy</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>4831673</td>\n",
       "            <td>1906933</td>\n",
       "            <td>8</td>\n",
       "            <td>Japan</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+---------+----------+--------------+----------+\n",
       "|    id   | movie_id | info_type_id |   info   |\n",
       "+---------+----------+--------------+----------+\n",
       "| 1631874 | 1102704  |     105      | $250,000 |\n",
       "| 9722642 | 2434417  |      1       |    60    |\n",
       "| 9503842 | 2098373  |      1       |    7     |\n",
       "| 4903218 | 1974739  |      8       |  Italy   |\n",
       "| 4831673 | 1906933  |      8       |  Japan   |\n",
       "+---------+----------+--------------+----------+"
      ]
     },
     "execution_count": 265,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_1d = %sqlcmd snippets query_1d\n",
    "grading_util.save_results(\"result_1d\", query_1d, result_1d)\n",
    "result_1d"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 266,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q1d</pre></strong> passed! 🌟</p>"
      ],
      "text/plain": [
       "q1d results: All test cases passed!"
      ]
     },
     "execution_count": 266,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q1d\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<br/><br/><br/>\n",
    "<hr style=\"border: 1px solid #fdb515;\" />\n",
    "\n",
    "\n",
    "# Question 2: Data Cleaning"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The `movie_sample` table contains a very minimal amount of information per movie:"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 267,
   "metadata": {},
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">5 rows affected.</span>"
      ],
      "text/plain": [
       "5 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>id</th>\n",
       "            <th>title</th>\n",
       "            <th>production_year</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>2038405</td>\n",
       "            <td>La corte de faraón</td>\n",
       "            <td>1944</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2081186</td>\n",
       "            <td>Long de xin</td>\n",
       "            <td>1985</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2177749</td>\n",
       "            <td>Onésime aime les bêtes</td>\n",
       "            <td>1913</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1718608</td>\n",
       "            <td>Bedtime Worries</td>\n",
       "            <td>1933</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2130699</td>\n",
       "            <td>Mothman</td>\n",
       "            <td>2000</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+---------+------------------------+-----------------+\n",
       "|    id   |         title          | production_year |\n",
       "+---------+------------------------+-----------------+\n",
       "| 2038405 |   La corte de faraón   |       1944      |\n",
       "| 2081186 |      Long de xin       |       1985      |\n",
       "| 2177749 | Onésime aime les bêtes |       1913      |\n",
       "| 1718608 |    Bedtime Worries     |       1933      |\n",
       "| 2130699 |        Mothman         |       2000      |\n",
       "+---------+------------------------+-----------------+"
      ]
     },
     "execution_count": 267,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "%sql SELECT * FROM movie_sample LIMIT 5;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "In this question, we’re going to create a nice, refined view of the `movie_sample` table that also includes a **rating field**, called `movie_ratings`.\n",
    "\n",
    "The [MPAA rating](https://www.motionpictures.org/film-ratings/) is commonly included in most datasets about movies, including ours, but in its current format in the dataset, it’s quite difficult to extract.\n",
    "\n",
    "The first clue about our approach comes from the random rows you explored in Question 1. As you saw, the `movie_info_sample` table contains a lot of information about each movie. Each row contains a particular type of information (e.g., runtime, languages) categorized by `info_type_id`. Based on the other tables in this database, the `info_type` table is a reference table to this \n",
    "ID number.\n",
    "\n",
    "Our strategy in this question is therefore as follows:\n",
    "* **Question 2a**: Find the `mpaa_rating_id` from the `info_type` table.\n",
    "* **Question 2b**: Extract the MPAA rating of a specific movie from the `movie_info_sample` table.\n",
    "* **Question 2c**: Construct a view `movie_ratings` based on the `movie_sample` table and all relevant MPAA ratings extracted from the `movie_info_sample` table."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 2a: MPAA Rating and `info_type`\n",
    "\n",
    "To start, using the `info_type` table, write a query to find which `id` corresponds to a film's MPAA rating.  The query `result_2a` that you write should return a relation with exactly one row and one attribute; the lone value in the instance should be the MPAA rating id number. We've then assigned `mpaa_rating_id` to extract the number itself from the relation.\n",
    "\n",
    "**Hints:** \n",
    "- Open the `psql` client in a terminal to explore the schema of `info_type` via the `\\d` metacommand (see the [Assignment Tips](https://fa23.data101.org/resources/assignment-tips/) page). Remember you can also write SQL commands to that terminal to interact with the IMDB database, but all final work must be submitted through this Jupyter Notebook.\n",
    "- Be careful when using quotes. SQL interprets single and double quotes differently. The single quote character `'` is reserved for delimiting string constants, while the double quote `\"` is used for naming tables or columns that require special characters. See [documentation](https://www.postgresql.org/docs/current/sql-syntax-lexical.html) for more."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 268,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">1 rows affected.</span>"
      ],
      "text/plain": [
       "1 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>id</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>97</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+----+\n",
       "| id |\n",
       "+----+\n",
       "| 97 |\n",
       "+----+"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/plain": [
       "97"
      ]
     },
     "execution_count": 268,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "result_2a = %sql SELECT id FROM info_type WHERE info= 'mpaa';\n",
    "# replace with %sql command\n",
    "mpaa_rating_id = result_2a[0][0]\n",
    "\n",
    "# do not edit below this line\n",
    "display(result_2a)\n",
    "mpaa_rating_id"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 269,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q2a</pre></strong> passed! 🙌</p>"
      ],
      "text/plain": [
       "q2a results: All test cases passed!"
      ]
     },
     "execution_count": 269,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q2a\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 2b: Looking up the MPAA Rating\n",
    "\n",
    "Suppose we wanted to find the MPAA rating for the 2004 American teen drama classic, _Mean Girls_. The below cell assigns `movie_id_2b` to the IMDb ID of this movie, 2109683."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 270,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">1 rows affected.</span>"
      ],
      "text/plain": [
       "1 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>id</th>\n",
       "            <th>title</th>\n",
       "            <th>production_year</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>2109683</td>\n",
       "            <td>Mean Girls</td>\n",
       "            <td>2004</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+---------+------------+-----------------+\n",
       "|    id   |   title    | production_year |\n",
       "+---------+------------+-----------------+\n",
       "| 2109683 | Mean Girls |       2004      |\n",
       "+---------+------------+-----------------+"
      ]
     },
     "execution_count": 270,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Just run this cell, no further action is needed.\n",
    "movie_id_2b = 2109683\n",
    "%sql SELECT * FROM movie_sample WHERE id = {{movie_id_2b}};"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "In the next cell, **write a query to find the MPAA rating for this movie.** Your query should return a relation with exactly one row , which has `(info, mpaa_rating)`, where `info` is the full MPAA rating string from `movie_info_sample`, and `mpaa_rating` is just the rating itself (i.e. `R`, `PG-13`, `PG`, etc) for this movie.\n",
    "\n",
    "\n",
    "**Before you get started**:\n",
    "* Explore the `movie_info_sample` tuples corresponding to the MPAA rating by using metacommands in the terminal. The `info` field is a little longer than just the rating. It also includes an explanation for why that movie received its rating. \n",
    "* You will need to extract a substring from the `info` column of `movie_info_sample`; you can use the [string functions](https://www.postgresql.org/docs/current/functions-string.html) in PostgreSQL to do it. There are many possible solutions. One possible solution is to use the substring function along with regex. If you use this approach, [this section on regex](https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-POSIX-REGEXP) may be particularly useful. [regex101.com](https://regex101.com) may also be helpful to craft your regular expressions.\n",
    "* You may use `mpaa_rating_id` and `movie_id_2b` directly in the rest of the questions using Python variable substitution (i.e., double curly braces). See the `JupySQL` documentation for more details."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 271,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">1 rows affected.</span>"
      ],
      "text/plain": [
       "1 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_2b result_2b << \n",
    "SELECT info, REGEXP_REPLACE(info, '^\\w+ ([a-zA-Z|\\d|-]+) .*$', '\\1') AS mpaa_rating\n",
    "FROM movie_info_sample\n",
    "WHERE movie_id = {{movie_id_2b}} AND info_type_id = {{mpaa_rating_id}};"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 272,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>info</th>\n",
       "            <th>mpaa_rating</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>Rated PG-13 for sexual content, language and some teen partying</td>\n",
       "            <td>PG-13</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+-----------------------------------------------------------------+-------------+\n",
       "|                               info                              | mpaa_rating |\n",
       "+-----------------------------------------------------------------+-------------+\n",
       "| Rated PG-13 for sexual content, language and some teen partying |    PG-13    |\n",
       "+-----------------------------------------------------------------+-------------+"
      ]
     },
     "execution_count": 272,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_2b = %sqlcmd snippets query_2b\n",
    "grading_util.save_results(\"result_2b\", query_2b, result_2b)\n",
    "result_2b"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 273,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q2b</pre></strong> passed! 🍀</p>"
      ],
      "text/plain": [
       "q2b results: All test cases passed!"
      ]
     },
     "execution_count": 273,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q2b\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "You may use `mpaa_rating_id` directly in the rest of the questions using python variable substitution.\n",
    "\n",
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 2c\n",
    "In the next cell,\n",
    "1. Construct a view named `movie_ratings` containing one row for each movie, which has `(movie_id, title, info, mpaa_rating)`, where `info` is the full MPAA rating string from `movie_info_sample`, and `mpaa_rating` is just the rating itself (i.e. `R`, `PG-13`, `PG`, etc).\n",
    "    * In other words, extend `movie_sample` with the MPAA rating attributes that you found in the previous question part, but this time for all movies.\n",
    "2. Following the view definition, also write a `SELECT` query to return the **first 20 rows** of the view, ordered by ascending `movie_id`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 274,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">20 rows affected.</span>"
      ],
      "text/plain": [
       "20 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_2c result_2c << \n",
    "DROP VIEW IF EXISTS movie_ratings;\n",
    "CREATE VIEW movie_ratings AS (\n",
    "    SELECT movie_id, title, info, REGEXP_REPLACE(info, '^\\w+ ([a-zA-Z|\\d|-]+) .*$', '\\1') AS mpaa_rating\n",
    "    FROM movie_info_sample \n",
    "        INNER JOIN movie_sample \n",
    "            ON movie_info_sample.movie_id = movie_sample.id\n",
    "    WHERE movie_info_sample.info_type_id = {{mpaa_rating_id}}\n",
    ");\n",
    "\n",
    "SELECT * FROM movie_ratings ORDER BY movie_id ASC LIMIT 20;\n",
    "\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 275,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>movie_id</th>\n",
       "            <th>title</th>\n",
       "            <th>info</th>\n",
       "            <th>mpaa_rating</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>1632926</td>\n",
       "            <td>$5 a Day</td>\n",
       "            <td>Rated PG-13 for sexual content, brief nudity and language</td>\n",
       "            <td>PG-13</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1632941</td>\n",
       "            <td>$9.99</td>\n",
       "            <td>Rated R for language and brief sexuality and nudity</td>\n",
       "            <td>R</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1632956</td>\n",
       "            <td>$windle</td>\n",
       "            <td>Rated R for some violence and brief sexuality/nudity</td>\n",
       "            <td>R</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1633013</td>\n",
       "            <td>'A' gai wak</td>\n",
       "            <td>Rated PG-13 for violence</td>\n",
       "            <td>PG-13</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1633014</td>\n",
       "            <td>'A' gai wak juk jap</td>\n",
       "            <td>Rated PG-13 for violence</td>\n",
       "            <td>PG-13</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1633461</td>\n",
       "            <td>'R Xmas</td>\n",
       "            <td>Rated R for strong language, drug content and some violence</td>\n",
       "            <td>R</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1633618</td>\n",
       "            <td>'Til There Was You</td>\n",
       "            <td>Rated PG-13 for sensuality, language and drug references</td>\n",
       "            <td>PG-13</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1633729</td>\n",
       "            <td>(500) Days of Summer</td>\n",
       "            <td>Rated PG-13 for sexual material and language</td>\n",
       "            <td>PG-13</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1633856</td>\n",
       "            <td>(Untitled)</td>\n",
       "            <td>Rated R for language and nude images</td>\n",
       "            <td>R</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1634282</td>\n",
       "            <td>.45</td>\n",
       "            <td>Rated R for pervasive strong language including graphic sexual references, violence, sexuality and some drug use</td>\n",
       "            <td>R</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+----------+----------------------+------------------------------------------------------------------------------------------------------------------+-------------+\n",
       "| movie_id |        title         |                                                       info                                                       | mpaa_rating |\n",
       "+----------+----------------------+------------------------------------------------------------------------------------------------------------------+-------------+\n",
       "| 1632926  |       $5 a Day       |                            Rated PG-13 for sexual content, brief nudity and language                             |    PG-13    |\n",
       "| 1632941  |        $9.99         |                               Rated R for language and brief sexuality and nudity                                |      R      |\n",
       "| 1632956  |       $windle        |                               Rated R for some violence and brief sexuality/nudity                               |      R      |\n",
       "| 1633013  |     'A' gai wak      |                                             Rated PG-13 for violence                                             |    PG-13    |\n",
       "| 1633014  | 'A' gai wak juk jap  |                                             Rated PG-13 for violence                                             |    PG-13    |\n",
       "| 1633461  |       'R Xmas        |                           Rated R for strong language, drug content and some violence                            |      R      |\n",
       "| 1633618  |  'Til There Was You  |                             Rated PG-13 for sensuality, language and drug references                             |    PG-13    |\n",
       "| 1633729  | (500) Days of Summer |                                   Rated PG-13 for sexual material and language                                   |    PG-13    |\n",
       "| 1633856  |      (Untitled)      |                                       Rated R for language and nude images                                       |      R      |\n",
       "| 1634282  |         .45          | Rated R for pervasive strong language including graphic sexual references, violence, sexuality and some drug use |      R      |\n",
       "+----------+----------------------+------------------------------------------------------------------------------------------------------------------+-------------+"
      ]
     },
     "execution_count": 275,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_2c = %sqlcmd snippets query_2c\n",
    "grading_util.save_results(\"result_2c\", query_2c, result_2c)\n",
    "result_2c"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 276,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q2c</pre></strong> passed! 🚀</p>"
      ],
      "text/plain": [
       "q2c results: All test cases passed!"
      ]
     },
     "execution_count": 276,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q2c\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br/><br/><br/>\n",
    "\n",
    "<hr style=\"border: 1px solid #fdb515;\" />\n",
    "\n",
    "# Question 3: Movie Moola\n",
    "One measure of a movie's success is how much money it makes. If we look at our `info_type` table, we have information about the film's gross earnings and the budget for a film. It would be nice to know how much money a film made using the profit formula:\n",
    "$$profit = earnings - moneyspent$$\n",
    "\n",
    "We start by taking a look at the gross info type, with `info_type_id = 107`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 277,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">10 rows affected.</span>"
      ],
      "text/plain": [
       "10 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>id</th>\n",
       "            <th>movie_id</th>\n",
       "            <th>info_type_id</th>\n",
       "            <th>info</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>1464348</td>\n",
       "            <td>2281091</td>\n",
       "            <td>107</td>\n",
       "            <td>INR 23,373,000 (India) (25 February 2005)</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1464349</td>\n",
       "            <td>2281091</td>\n",
       "            <td>107</td>\n",
       "            <td>INR 19,207,000 (India) (18 February 2005)</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1464374</td>\n",
       "            <td>1766950</td>\n",
       "            <td>107</td>\n",
       "            <td>HKD 826,364 (Hong Kong) (11 December 1975)</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1464375</td>\n",
       "            <td>1769023</td>\n",
       "            <td>107</td>\n",
       "            <td>HKD 3,148,549 (Hong Kong) (19 November 1980)</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1464378</td>\n",
       "            <td>1799099</td>\n",
       "            <td>107</td>\n",
       "            <td>HKD 6,493,694 (Hong Kong) (22 December 1981)</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1464383</td>\n",
       "            <td>1847670</td>\n",
       "            <td>107</td>\n",
       "            <td>$21,438 (USA) (9 August 2009)</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1464384</td>\n",
       "            <td>1847670</td>\n",
       "            <td>107</td>\n",
       "            <td>$10,266 (USA) (2 August 2009)</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1464396</td>\n",
       "            <td>1916002</td>\n",
       "            <td>107</td>\n",
       "            <td>$5,932 (USA) (27 November 2005)</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1464397</td>\n",
       "            <td>1916002</td>\n",
       "            <td>107</td>\n",
       "            <td>$4,206 (USA) (20 November 2005)</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1464398</td>\n",
       "            <td>1916002</td>\n",
       "            <td>107</td>\n",
       "            <td>$2,939 (USA) (23 October 2005)</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>\n",
       "<span style=\"font-style:italic;text-align:center;\">Truncated to <a href=\"https://jupysql.ploomber.io/en/latest/api/configuration.html#displaylimit\">displaylimit</a> of 10.</span>"
      ],
      "text/plain": [
       "+---------+----------+--------------+----------------------------------------------+\n",
       "|    id   | movie_id | info_type_id |                     info                     |\n",
       "+---------+----------+--------------+----------------------------------------------+\n",
       "| 1464348 | 2281091  |     107      |  INR 23,373,000 (India) (25 February 2005)   |\n",
       "| 1464349 | 2281091  |     107      |  INR 19,207,000 (India) (18 February 2005)   |\n",
       "| 1464374 | 1766950  |     107      |  HKD 826,364 (Hong Kong) (11 December 1975)  |\n",
       "| 1464375 | 1769023  |     107      | HKD 3,148,549 (Hong Kong) (19 November 1980) |\n",
       "| 1464378 | 1799099  |     107      | HKD 6,493,694 (Hong Kong) (22 December 1981) |\n",
       "| 1464383 | 1847670  |     107      |        $21,438 (USA) (9 August 2009)         |\n",
       "| 1464384 | 1847670  |     107      |        $10,266 (USA) (2 August 2009)         |\n",
       "| 1464396 | 1916002  |     107      |       $5,932 (USA) (27 November 2005)        |\n",
       "| 1464397 | 1916002  |     107      |       $4,206 (USA) (20 November 2005)        |\n",
       "| 1464398 | 1916002  |     107      |        $2,939 (USA) (23 October 2005)        |\n",
       "+---------+----------+--------------+----------------------------------------------+\n",
       "Truncated to displaylimit of 10."
      ]
     },
     "execution_count": 277,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "%%sql\n",
    "SELECT * \n",
    "FROM movie_info_sample\n",
    "WHERE info_type_id = 107\n",
    "ORDER BY id\n",
    "LIMIT 10 OFFSET 100000;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "There are a lot of things to notice here. First of all, the values in the `info` attribute are strings with not only the earnings, but also the country and the month the earnings are cummulatively summed until. Additionally, the info values are not all in the same currency! On top of that, it appears as if some of the gross earnings, even for those in USD are from worldwide sales, while others only count sales within the USA.\n",
    "\n",
    "For consistency, let's only use movies with gross earnings counted in the USA and that are in US Dollars ($). "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 3a: Earnings\n",
    "We want the numerical part of the `info` column and the **maximum earnings value** for a particular film. \n",
    "\n",
    "In the next cell,\n",
    "- Construct a view named `movie_gross` containing one row for each movie, which has `(gross, movie_id, title)`, where `gross` is the numeric dollar amount extracted as a float.\n",
    "- To take a look at our cleaned data, write a `SELECT` query to display the **top 10 highest grossing films** from `movie_gross`.\n",
    "\n",
    "**Hints:** \n",
    "- The way we extracted the MPAA rating is very similar to how we want to isolate the numeric dollar amount as a string. (There are multiple ways of doing this.)\n",
    "- Look at the [documentation](https://www.postgresql.org/docs/9.4/functions-matching.html) for the `regexp_replace` function, and specifically 'flag g'.\n",
    "- The staff solution found it helpful to make an additional subview."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 278,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">10 rows affected.</span>"
      ],
      "text/plain": [
       "10 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_3a result_3a <<\n",
    "\n",
    "DROP VIEW IF EXISTS raw_gross CASCADE;\n",
    "CREATE VIEW raw_gross AS(\n",
    "    SELECT CAST(REGEXP_REPLACE(info, ',|\\$| \\(.*\\)', '', 'g') AS FLOAT) AS gross, movie_id, m.title AS title\n",
    "    FROM movie_info_sample as i\n",
    "    INNER JOIN movie_sample as m\n",
    "        ON i.movie_id = m.id\n",
    "    WHERE info_type_id = 107 \n",
    "            AND info LIKE '%(USA)%' \n",
    "            AND info LIKE '%$%'\n",
    ");\n",
    "DROP VIEW IF EXISTS movie_gross;\n",
    "CREATE VIEW movie_gross AS(\n",
    "    SELECT MAX(gross) AS gross, movie_id, title\n",
    "    FROM raw_gross\n",
    "    GROUP BY movie_id, title\n",
    ");\n",
    "\n",
    "SELECT *\n",
    "FROM movie_gross\n",
    "ORDER BY gross DESC\n",
    "LIMIT 10;\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 279,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>gross</th>\n",
       "            <th>movie_id</th>\n",
       "            <th>title</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>760507625.0</td>\n",
       "            <td>1704289</td>\n",
       "            <td>Avatar</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>658672302.0</td>\n",
       "            <td>2438179</td>\n",
       "            <td>Titanic</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>623357910.0</td>\n",
       "            <td>2346436</td>\n",
       "            <td>The Avengers</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>534858444.0</td>\n",
       "            <td>2360583</td>\n",
       "            <td>The Dark Knight</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>460935665.0</td>\n",
       "            <td>2310522</td>\n",
       "            <td>Star Wars</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>448139099.0</td>\n",
       "            <td>2360588</td>\n",
       "            <td>The Dark Knight Rises</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>436471036.0</td>\n",
       "            <td>2285018</td>\n",
       "            <td>Shrek 2</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>435110554.0</td>\n",
       "            <td>1851357</td>\n",
       "            <td>E.T. the Extra-Terrestrial</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>431065444.0</td>\n",
       "            <td>2310573</td>\n",
       "            <td>Star Wars: Episode I - The Phantom Menace</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>423315812.0</td>\n",
       "            <td>2204345</td>\n",
       "            <td>Pirates of the Caribbean: Dead Man's Chest</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+-------------+----------+--------------------------------------------+\n",
       "|    gross    | movie_id |                   title                    |\n",
       "+-------------+----------+--------------------------------------------+\n",
       "| 760507625.0 | 1704289  |                   Avatar                   |\n",
       "| 658672302.0 | 2438179  |                  Titanic                   |\n",
       "| 623357910.0 | 2346436  |                The Avengers                |\n",
       "| 534858444.0 | 2360583  |              The Dark Knight               |\n",
       "| 460935665.0 | 2310522  |                 Star Wars                  |\n",
       "| 448139099.0 | 2360588  |           The Dark Knight Rises            |\n",
       "| 436471036.0 | 2285018  |                  Shrek 2                   |\n",
       "| 435110554.0 | 1851357  |         E.T. the Extra-Terrestrial         |\n",
       "| 431065444.0 | 2310573  | Star Wars: Episode I - The Phantom Menace  |\n",
       "| 423315812.0 | 2204345  | Pirates of the Caribbean: Dead Man's Chest |\n",
       "+-------------+----------+--------------------------------------------+"
      ]
     },
     "execution_count": 279,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_3a = %sqlcmd snippets query_3a\n",
    "grading_util.save_results(\"result_3a\", query_3a, result_3a)\n",
    "result_3a"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 280,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q3a</pre></strong> passed! 🎉</p>"
      ],
      "text/plain": [
       "q3a results: All test cases passed!"
      ]
     },
     "execution_count": 280,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q3a\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<br/>\n",
    "\n",
    "---\n",
    "\n",
    "## Tutorial: Budget\n",
    "We will now look at the budget info type, with `info_type_id = 105`."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 281,
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">10 rows affected.</span>"
      ],
      "text/plain": [
       "10 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>id</th>\n",
       "            <th>movie_id</th>\n",
       "            <th>info_type_id</th>\n",
       "            <th>info</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>1261074</td>\n",
       "            <td>1983149</td>\n",
       "            <td>105</td>\n",
       "            <td>$75,000,000</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1261110</td>\n",
       "            <td>1983269</td>\n",
       "            <td>105</td>\n",
       "            <td>INR 180,000,000</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1261160</td>\n",
       "            <td>2381188</td>\n",
       "            <td>105</td>\n",
       "            <td>$40,000,000</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1261170</td>\n",
       "            <td>1991083</td>\n",
       "            <td>105</td>\n",
       "            <td>FIM 9,219,499</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1261210</td>\n",
       "            <td>1993907</td>\n",
       "            <td>105</td>\n",
       "            <td>$45,000,000</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1261247</td>\n",
       "            <td>1995787</td>\n",
       "            <td>105</td>\n",
       "            <td>$38,000,000</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1261308</td>\n",
       "            <td>1999081</td>\n",
       "            <td>105</td>\n",
       "            <td>$50,000,000</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1261324</td>\n",
       "            <td>1999196</td>\n",
       "            <td>105</td>\n",
       "            <td>SEK 40,000,000</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1261375</td>\n",
       "            <td>2001114</td>\n",
       "            <td>105</td>\n",
       "            <td>$60,000,000</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1261396</td>\n",
       "            <td>2001989</td>\n",
       "            <td>105</td>\n",
       "            <td>$13,000,000</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>\n",
       "<span style=\"font-style:italic;text-align:center;\">Truncated to <a href=\"https://jupysql.ploomber.io/en/latest/api/configuration.html#displaylimit\">displaylimit</a> of 10.</span>"
      ],
      "text/plain": [
       "+---------+----------+--------------+-----------------+\n",
       "|    id   | movie_id | info_type_id |       info      |\n",
       "+---------+----------+--------------+-----------------+\n",
       "| 1261074 | 1983149  |     105      |   $75,000,000   |\n",
       "| 1261110 | 1983269  |     105      | INR 180,000,000 |\n",
       "| 1261160 | 2381188  |     105      |   $40,000,000   |\n",
       "| 1261170 | 1991083  |     105      |  FIM 9,219,499  |\n",
       "| 1261210 | 1993907  |     105      |   $45,000,000   |\n",
       "| 1261247 | 1995787  |     105      |   $38,000,000   |\n",
       "| 1261308 | 1999081  |     105      |   $50,000,000   |\n",
       "| 1261324 | 1999196  |     105      |  SEK 40,000,000 |\n",
       "| 1261375 | 2001114  |     105      |   $60,000,000   |\n",
       "| 1261396 | 2001989  |     105      |   $13,000,000   |\n",
       "+---------+----------+--------------+-----------------+\n",
       "Truncated to displaylimit of 10."
      ]
     },
     "execution_count": 281,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "%%sql\n",
    "SELECT * \n",
    "FROM movie_info_sample\n",
    "WHERE info_type_id = 105\n",
    "ORDER BY id\n",
    "LIMIT 10 OFFSET 5000;"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "Similar to when we examined the gross info, we see a lot of non-US dollar currencies. For consistency, let's only use movies **with a budget in US dollars.**"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "## Question 3b:"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "Now, we want something similar for the budget of the film, so that we can perform the subtraction of `gross` and `budget`. We want the numerical part of the `info` column and the **maximum budget value** for a particular film (as you can verify, some movies have more than one budget). \n",
    "\n",
    "In the next cell,\n",
    "- Construct a view named `movie_budget` containing one row for each movie, which has `(budget, movie_id, title)`, where `budget` is the numeric dollar amount extracted as a float.\n",
    "- To take a look at our cleaned data, write a `SELECT` query to display the **top 10 highest budget films** from `movie_budget`. When multiple films have the same budget, break ties by `movie_id` (ascending).\n",
    "\n",
    "**Hint:** The query here should be quite similar to Question 3a. Make sure to break ties properly!"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 282,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">10 rows affected.</span>"
      ],
      "text/plain": [
       "10 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_3b result_3b <<\n",
    "\n",
    "DROP VIEW IF EXISTS raw_budget CASCADE;\n",
    "CREATE VIEW raw_budget AS(\n",
    "    SELECT CAST(REGEXP_REPLACE(info, '[\\D]', '', 'g') AS FLOAT) AS budget, movie_id, m.title AS title\n",
    "    FROM movie_info_sample as i\n",
    "    INNER JOIN movie_sample as m\n",
    "        ON i.movie_id = m.id\n",
    "    WHERE info_type_id = 105 \n",
    "            AND info LIKE '%$%'\n",
    ");\n",
    "DROP VIEW IF EXISTS movie_budget;\n",
    "CREATE VIEW movie_budget AS(\n",
    "    SELECT MAX(budget) AS budget, movie_id, title\n",
    "    FROM raw_budget\n",
    "    GROUP BY movie_id, title\n",
    "    ORDER BY movie_id ASC\n",
    ");\n",
    "\n",
    "SELECT *\n",
    "FROM movie_budget\n",
    "ORDER BY budget DESC\n",
    "LIMIT 10;\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 283,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>budget</th>\n",
       "            <th>movie_id</th>\n",
       "            <th>title</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>300000000.0</td>\n",
       "            <td>2204343</td>\n",
       "            <td>Pirates of the Caribbean: At World's End</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>260000000.0</td>\n",
       "            <td>2332419</td>\n",
       "            <td>Tangled</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>258000000.0</td>\n",
       "            <td>2305993</td>\n",
       "            <td>Spider-Man 3</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>250000000.0</td>\n",
       "            <td>2360588</td>\n",
       "            <td>The Dark Knight Rises</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>250000000.0</td>\n",
       "            <td>2204347</td>\n",
       "            <td>Pirates of the Caribbean: On Stranger Tides</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>250000000.0</td>\n",
       "            <td>2387922</td>\n",
       "            <td>The Lone Ranger</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>250000000.0</td>\n",
       "            <td>1938937</td>\n",
       "            <td>Harry Potter and the Half-Blood Prince</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>250000000.0</td>\n",
       "            <td>2002374</td>\n",
       "            <td>John Carter</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>237000000.0</td>\n",
       "            <td>1704289</td>\n",
       "            <td>Avatar</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>230000000.0</td>\n",
       "            <td>2344435</td>\n",
       "            <td>The Amazing Spider-Man</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+-------------+----------+---------------------------------------------+\n",
       "|    budget   | movie_id |                    title                    |\n",
       "+-------------+----------+---------------------------------------------+\n",
       "| 300000000.0 | 2204343  |   Pirates of the Caribbean: At World's End  |\n",
       "| 260000000.0 | 2332419  |                   Tangled                   |\n",
       "| 258000000.0 | 2305993  |                 Spider-Man 3                |\n",
       "| 250000000.0 | 2360588  |            The Dark Knight Rises            |\n",
       "| 250000000.0 | 2204347  | Pirates of the Caribbean: On Stranger Tides |\n",
       "| 250000000.0 | 2387922  |               The Lone Ranger               |\n",
       "| 250000000.0 | 1938937  |    Harry Potter and the Half-Blood Prince   |\n",
       "| 250000000.0 | 2002374  |                 John Carter                 |\n",
       "| 237000000.0 | 1704289  |                    Avatar                   |\n",
       "| 230000000.0 | 2344435  |            The Amazing Spider-Man           |\n",
       "+-------------+----------+---------------------------------------------+"
      ]
     },
     "execution_count": 283,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_3b = %sqlcmd snippets query_3b\n",
    "grading_util.save_results(\"result_3b\", query_3b, result_3b)\n",
    "result_3b"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 284,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q3b</pre></strong> passed! 🌈</p>"
      ],
      "text/plain": [
       "q3b results: All test cases passed!"
      ]
     },
     "execution_count": 284,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q3b\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 3c\n",
    "\n",
    "We have all the parts we need to calculate the profits. Using the `movie_gross` and `movie_budget` views created above, we can now subtract the numeric columns and save the result in another column called `profit`.\n",
    "\n",
    "In the next cell, construct a view named `movie_profit` containing one row for each movie, which has `(movie_id, title, profit)`, where `profit` is the result of subtracting that movie's `budget` from `gross`. Following the view definition, write a `SELECT` query to return the **first 10 rows** of the view ordered by descending `profit`. This may take a while to execute."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 285,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">10 rows affected.</span>"
      ],
      "text/plain": [
       "10 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_3c result_3c <<\n",
    "\n",
    "DROP VIEW IF EXISTS movie_profit;\n",
    "CREATE VIEW movie_profit AS(\n",
    "    SELECT g.movie_id AS movie_id, g.title AS title, (g.gross - b.budget) AS profit\n",
    "    FROM movie_gross AS g\n",
    "    JOIN movie_budget AS b ON g.movie_id = b.movie_id\n",
    ");\n",
    "SELECT * \n",
    "FROM movie_profit \n",
    "ORDER BY profit DESC\n",
    "LIMIT 10;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 286,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>movie_id</th>\n",
       "            <th>title</th>\n",
       "            <th>profit</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>1704289</td>\n",
       "            <td>Avatar</td>\n",
       "            <td>523507625.0</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2438179</td>\n",
       "            <td>Titanic</td>\n",
       "            <td>458672302.0</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2310522</td>\n",
       "            <td>Star Wars</td>\n",
       "            <td>449935665.0</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1851357</td>\n",
       "            <td>E.T. the Extra-Terrestrial</td>\n",
       "            <td>424610554.0</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2346436</td>\n",
       "            <td>The Avengers</td>\n",
       "            <td>403357910.0</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2360583</td>\n",
       "            <td>The Dark Knight</td>\n",
       "            <td>349858444.0</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2400712</td>\n",
       "            <td>The Passion of the Christ</td>\n",
       "            <td>340782930.0</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2006991</td>\n",
       "            <td>Jurassic Park</td>\n",
       "            <td>338820792.0</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2172509</td>\n",
       "            <td>Olympus Has Fallen</td>\n",
       "            <td>330824682.0</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2379293</td>\n",
       "            <td>The Hunger Games</td>\n",
       "            <td>330010692.0</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+----------+----------------------------+-------------+\n",
       "| movie_id |           title            |    profit   |\n",
       "+----------+----------------------------+-------------+\n",
       "| 1704289  |           Avatar           | 523507625.0 |\n",
       "| 2438179  |          Titanic           | 458672302.0 |\n",
       "| 2310522  |         Star Wars          | 449935665.0 |\n",
       "| 1851357  | E.T. the Extra-Terrestrial | 424610554.0 |\n",
       "| 2346436  |        The Avengers        | 403357910.0 |\n",
       "| 2360583  |      The Dark Knight       | 349858444.0 |\n",
       "| 2400712  | The Passion of the Christ  | 340782930.0 |\n",
       "| 2006991  |       Jurassic Park        | 338820792.0 |\n",
       "| 2172509  |     Olympus Has Fallen     | 330824682.0 |\n",
       "| 2379293  |      The Hunger Games      | 330010692.0 |\n",
       "+----------+----------------------------+-------------+"
      ]
     },
     "execution_count": 286,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_3c = %sqlcmd snippets query_3c\n",
    "grading_util.save_results(\"result_3c\", query_3c, result_3c)\n",
    "result_3c"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 287,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q3c</pre></strong> passed! 🙌</p>"
      ],
      "text/plain": [
       "q3c results: All test cases passed!"
      ]
     },
     "execution_count": 287,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q3c\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<!-- BEGIN QUESTION -->\n",
    "\n",
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 3d\n",
    "\n",
    "We analyzed the data, but something seems odd. Upon closer look, there are many negative values for `profit`. For example, the movie `102 Dalmations` looks to have lost around $18M, but it was a widely successful film! What may account for this issue? Think about how we constrained our data from the start of the problem."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "One of the reason behind this is we constrained our gross to only in the USA, so we will miss information about sells in dollars but not in the USA. "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<!-- END QUESTION -->\n",
    "\n",
    "<br/><br/><br/>\n",
    "\n",
    "<hr style=\"border: 1px solid #fdb515;\" />\n",
    "\n",
    "# Question 4: Using Cleaned Data\n",
    "\n",
    "Now that we have cleaned our monetary records from the `info` attribute in `movie_info_sample`, let's take a closer look at the data we generated. "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 4a: Earnings per Genre\n",
    "\n",
    "Another `info_type` we can look at is the movie genre. Looking at the `movie_gross` values, how much does each *genre* earn on average in the US?\n",
    "\n",
    "- Create a view with the columns `movie_id`, `title`, `gross`, `genre`, and `average_genre` where `gross` is a movie's gross US earnings, `genre` is the movie's genre, and `average_genre` is the average earnings for the corresponding genre. If a movie has multiple genres, the movie should appear in multiple rows with each genre as a row.\n",
    "\n",
    "- Following the view definition, write a `SELECT` query to return the rows for the movie \"Mr. & Mrs. Smith\" ordered by genre alphabetically.\n",
    "\n",
    "**Hint:** Look into [window functions](https://www.postgresql.org/docs/9.1/tutorial-window.html)\n"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 288,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">3 rows affected.</span>"
      ],
      "text/plain": [
       "3 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_4a result_4a <<\n",
    "DROP VIEW IF EXISTS movie_genre CASCADE;\n",
    "CREATE VIEW movie_genre AS(\n",
    "    SELECT movie_id, info AS genre\n",
    "    FROM movie_info_sample\n",
    "    WHERE info_type_id = 3\n",
    ");\n",
    "\n",
    "DROP VIEW IF EXISTS movie_avg_genre;\n",
    "CREATE VIEW movie_avg_genre AS(\n",
    "    SELECT m.movie_id AS movie_id, title, gross, genre,\n",
    "            AVG(gross) OVER (PARTITION BY genre) AS average_genre\n",
    "    FROM movie_gross as m\n",
    "    INNER JOIN movie_genre as g\n",
    "    ON m.movie_id = g.movie_id\n",
    "    \n",
    ");\n",
    "\n",
    "SELECT * FROM movie_avg_genre\n",
    "WHERE title ='Mr. & Mrs. Smith'\n",
    "ORDER BY genre;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 289,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>movie_id</th>\n",
       "            <th>title</th>\n",
       "            <th>gross</th>\n",
       "            <th>genre</th>\n",
       "            <th>average_genre</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>2132092</td>\n",
       "            <td>Mr. & Mrs. Smith</td>\n",
       "            <td>186336103.0</td>\n",
       "            <td>Action</td>\n",
       "            <td>42123826.131625965</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2132092</td>\n",
       "            <td>Mr. & Mrs. Smith</td>\n",
       "            <td>186336103.0</td>\n",
       "            <td>Comedy</td>\n",
       "            <td>21583843.81801513</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>2132092</td>\n",
       "            <td>Mr. & Mrs. Smith</td>\n",
       "            <td>186336103.0</td>\n",
       "            <td>Romance</td>\n",
       "            <td>18470817.081399772</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+----------+------------------+-------------+---------+--------------------+\n",
       "| movie_id |      title       |    gross    |  genre  |   average_genre    |\n",
       "+----------+------------------+-------------+---------+--------------------+\n",
       "| 2132092  | Mr. & Mrs. Smith | 186336103.0 |  Action | 42123826.131625965 |\n",
       "| 2132092  | Mr. & Mrs. Smith | 186336103.0 |  Comedy | 21583843.81801513  |\n",
       "| 2132092  | Mr. & Mrs. Smith | 186336103.0 | Romance | 18470817.081399772 |\n",
       "+----------+------------------+-------------+---------+--------------------+"
      ]
     },
     "execution_count": 289,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_4a = %sqlcmd snippets query_4a\n",
    "grading_util.save_results(\"result_4a\", query_4a, result_4a)\n",
    "result_4a"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 290,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q4a</pre></strong> passed! 🙌</p>"
      ],
      "text/plain": [
       "q4a results: All test cases passed!"
      ]
     },
     "execution_count": 290,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q4a\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 4b: Analyzing Gross Earnings\n",
    "\n",
    "A common way to view numerical data is with a boxplot. A boxplot shows a spread of the data along with several other key attributes that allow for further data analysis. "
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "We went through a lot of work transforming the gross earnings from strings in the`info` attribute into a numerical value. Because of our hard work, we can now further examine this data and understand its distribution. To do this, we first need to generate a [five-number summary](https://en.wikipedia.org/wiki/Five-number_summary) and find the average of the US gross earnings data.\n",
    "\n",
    "- Create a view named `earnings_summary`, which consists of a one row summary of the `movie_gross` `gross` data with the `min`, `25th_percentile`, `median`, `75th_percentile`, `max`, and `average`. \n",
    "- Following the view definition, write a `SELECT` query to display it.\n",
    "\n",
    "**Hint:** Look at SQL [aggregate functions](https://www.postgresql.org/docs/9.4/functions-aggregate.html). You may find some useful."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 291,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">1 rows affected.</span>"
      ],
      "text/plain": [
       "1 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_4b result_4b <<\n",
    "\n",
    "DROP VIEW IF EXISTS earnings_summary;\n",
    "CREATE VIEW earnings_summary AS(\n",
    "    SELECT PERCENTILE_CONT(0) WITHIN GROUP (ORDER BY gross) AS min, \n",
    "        PERCENTILE_CONT(0.25) WITHIN GROUP(ORDER BY gross) AS \"25th_percentile\",\n",
    "        PERCENTILE_CONT(0.5) WITHIN GROUP(ORDER BY gross) AS median,\n",
    "        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY gross) AS \"75th_percentile\",\n",
    "        PERCENTILE_CONT(1) WITHIN GROUP (ORDER BY gross) AS max,\n",
    "        AVG(gross) AS average\n",
    "    FROM movie_gross\n",
    "    LIMIT 1\n",
    ");\n",
    "SELECT * \n",
    "FROM earnings_summary\n",
    "LIMIT 2;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 292,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>min</th>\n",
       "            <th>25th_percentile</th>\n",
       "            <th>median</th>\n",
       "            <th>75th_percentile</th>\n",
       "            <th>max</th>\n",
       "            <th>average</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>30.0</td>\n",
       "            <td>166623.0</td>\n",
       "            <td>2317091.0</td>\n",
       "            <td>20002717.5</td>\n",
       "            <td>760507625.0</td>\n",
       "            <td>19594424.63641884</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+------+-----------------+-----------+-----------------+-------------+-------------------+\n",
       "| min  | 25th_percentile |   median  | 75th_percentile |     max     |      average      |\n",
       "+------+-----------------+-----------+-----------------+-------------+-------------------+\n",
       "| 30.0 |     166623.0    | 2317091.0 |    20002717.5   | 760507625.0 | 19594424.63641884 |\n",
       "+------+-----------------+-----------+-----------------+-------------+-------------------+"
      ]
     },
     "execution_count": 292,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_4b = %sqlcmd snippets query_4b\n",
    "grading_util.save_results(\"result_4b\", query_4b, result_4b)\n",
    "result_4b"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 293,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q4b</pre></strong> passed! 🍀</p>"
      ],
      "text/plain": [
       "q4b results: All test cases passed!"
      ]
     },
     "execution_count": 293,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q4b\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<!-- BEGIN QUESTION -->\n",
    "\n",
    "<br><br>\n",
    "\n",
    "---\n",
    "\n",
    "## Question 4c\n",
    "What do you notice about the summary values generated in `earnings_summary`? We can represent the five-number summary graphically using a [box plot](https://en.wikipedia.org/wiki/Box_plot). Identify two properties about the boxplot of the data. (You do not need to explicitly create a boxplot, but think about how the summary statistics would be distributed in a boxplot.)\n",
    "\n",
    "**Hint:** Think in terms of about concepts from statistics like spread, modality, skew, etc. and how they may apply here."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The first property about the boxplot of the data is that this is heavily skewed to the right with most of the data concentrate around small value and few data with big value. This can be justified by looking at the mean (19 millions) is significantly higher than the median(2 millions). Another property is this boxplot has a large spread with min equals 30 and max is around 760 millions."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 298,
   "metadata": {},
   "outputs": [],
   "source": [
    "# %%sql --save query_4c<<\n",
    "# SELECT * FROM movie_gross;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 295,
   "metadata": {},
   "outputs": [],
   "source": [
    "# query_4c = %sqlcmd snippets query_4c"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 296,
   "metadata": {
    "tags": []
   },
   "outputs": [],
   "source": [
    "# optional: include your plotting code here\n",
    "# import pandas as pd\n",
    "# import seaborn as sns\n",
    "# import matplotlib.pyplot as plt\n",
    "\n",
    "# gross = pd.read_sql(query_4c, 'postgresql://jovyan@127.0.0.1:5432/imdb')\n",
    "# sns.boxplot(x=gross['gross'])\n",
    "# plt.axvline(x=np.mean(gross['gross']), color='r')\n",
    "# plt.title('Boxplot for Gross')\n",
    "# plt.show()\n"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "<!-- END QUESTION -->\n",
    "\n",
    "<br/><br/><br/>\n",
    "\n",
    "<hr style=\"border: 1px solid #fdb515;\" />\n",
    "\n",
    "# Question 5: Joins\n",
    "\n",
    "Joins are a powerful tool in database cleaning and analysis. They allow for the user to create useful tables and bring together information in a meaningful way. \n",
    "\n",
    "There are many types of joins: inner, outer, left, right, etc. Let's practice these in a special scenario. \n",
    "\n",
    "You are now working as a talent director and you need a list of all people who have been in `actor` roles and the number of movies in which they have acted. \n",
    "\n",
    "- Create a view called `number_movies`, which has columns `id`, `name`, `number` where `id` is the actor's id, `name` is the actor's name, and `number` is the number of movies they have acted in.\n",
    "- Following your view, write a ``SELECT`` query to display the **top 10 actors** who have been in the most films.\n",
    "\n",
    "**Note:** The `cast_sample` may include actors not included in `actor_sample` table. We still want to include these actors in our result by reference to their id. The `name` field can be NULL."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 299,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<span style=\"None\">Running query in &#x27;postgresql://jovyan@127.0.0.1:5432/imdb&#x27;</span>"
      ],
      "text/plain": [
       "Running query in 'postgresql://jovyan@127.0.0.1:5432/imdb'"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    },
    {
     "data": {
      "text/html": [
       "<span style=\"color: green\">10 rows affected.</span>"
      ],
      "text/plain": [
       "10 rows affected."
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "%%sql --save query_5 result_5 <<\n",
    "\n",
    "DROP VIEW IF EXISTS number_movies;\n",
    "CREATE VIEW number_movies AS (\n",
    "    SELECT DISTINCT c.person_id AS id, a.name AS name, COUNT(movie_id) OVER(PARTITION BY c.person_id) AS number\n",
    "    FROM cast_sample AS c\n",
    "    LEFT JOIN actor_sample AS a\n",
    "        ON c.person_id = a.id\n",
    "    WHERE c.role_id = 1\n",
    ");\n",
    "SELECT *\n",
    "FROM number_movies\n",
    "ORDER BY number DESC\n",
    "LIMIT 10;"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 300,
   "metadata": {
    "tags": []
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<table>\n",
       "    <thead>\n",
       "        <tr>\n",
       "            <th>id</th>\n",
       "            <th>name</th>\n",
       "            <th>number</th>\n",
       "        </tr>\n",
       "    </thead>\n",
       "    <tbody>\n",
       "        <tr>\n",
       "            <td>95397</td>\n",
       "            <td>Barker, Bob</td>\n",
       "            <td>6853</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>515315</td>\n",
       "            <td>Freeman, Morgan</td>\n",
       "            <td>5938</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>677696</td>\n",
       "            <td>Hinnant, Skip</td>\n",
       "            <td>4697</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1573853</td>\n",
       "            <td>Trebek, Alex</td>\n",
       "            <td>4690</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1362169</td>\n",
       "            <td>Sajak, Pat</td>\n",
       "            <td>3937</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>1417394</td>\n",
       "            <td>Shaffer, Paul</td>\n",
       "            <td>3546</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>911160</td>\n",
       "            <td>Lima, Pedro</td>\n",
       "            <td>2911</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>900749</td>\n",
       "            <td>Letterman, David</td>\n",
       "            <td>2895</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>487253</td>\n",
       "            <td>Filipe, Guilherme</td>\n",
       "            <td>2861</td>\n",
       "        </tr>\n",
       "        <tr>\n",
       "            <td>356575</td>\n",
       "            <td>Davidson, Doug</td>\n",
       "            <td>2760</td>\n",
       "        </tr>\n",
       "    </tbody>\n",
       "</table>"
      ],
      "text/plain": [
       "+---------+-------------------+--------+\n",
       "|    id   |        name       | number |\n",
       "+---------+-------------------+--------+\n",
       "|  95397  |    Barker, Bob    |  6853  |\n",
       "|  515315 |  Freeman, Morgan  |  5938  |\n",
       "|  677696 |   Hinnant, Skip   |  4697  |\n",
       "| 1573853 |    Trebek, Alex   |  4690  |\n",
       "| 1362169 |     Sajak, Pat    |  3937  |\n",
       "| 1417394 |   Shaffer, Paul   |  3546  |\n",
       "|  911160 |    Lima, Pedro    |  2911  |\n",
       "|  900749 |  Letterman, David |  2895  |\n",
       "|  487253 | Filipe, Guilherme |  2861  |\n",
       "|  356575 |   Davidson, Doug  |  2760  |\n",
       "+---------+-------------------+--------+"
      ]
     },
     "execution_count": 300,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "# Do not delete/edit this cell!\n",
    "# You must run this cell before running the autograder.\n",
    "query_5 = %sqlcmd snippets query_5\n",
    "grading_util.save_results(\"result_5\", query_5, result_5)\n",
    "result_5"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 301,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "data": {
      "text/html": [
       "<p><strong><pre style='display: inline;'>q5</pre></strong> passed! 💯</p>"
      ],
      "text/plain": [
       "q5 results: All test cases passed!"
      ]
     },
     "execution_count": 301,
     "metadata": {},
     "output_type": "execute_result"
    }
   ],
   "source": [
    "grader.check(\"q5\")"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "<hr style=\"border: 5px solid #003262;\" />\n",
    "<hr style=\"border: 1px solid #fdb515;\" />"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "# Congratulations! You have finished Project 1."
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    "The below code prepares all the additional files needed for your submission, including:\n",
    "* `results.zip`\n",
    "* `proj1.pdf`\n",
    "\n",
    "**Make sure to run this cell before exporting the final zip file with `grader.export()`!**"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "metadata": {
    "scrolled": true
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "/srv/conda/envs/notebook/lib/python3.11/site-packages/nbconvert/utils/pandoc.py:51: RuntimeWarning: You are using an unsupported version of pandoc (2.12).\n",
      "Your version must be at least (2.14.2) but less than (4.0.0).\n",
      "Refer to https://pandoc.org/installing.html.\n",
      "Continuing with doubts...\n",
      "  check_pandoc_version()\n"
     ]
    }
   ],
   "source": [
    "grading_util.prepare_submission_and_cleanup() \n",
    "!otter export -e latex proj1.ipynb proj1.pdf   # Export PDF"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 242,
   "metadata": {},
   "outputs": [
    {
     "name": "stderr",
     "output_type": "stream",
     "text": [
      "RuntimeError: Could not close connection because it was not found amongst these: ['postgresql://jovyan@127.0.0.1:5432/imdb']\n",
      "If you need help solving this issue, send us a message: https://ploomber.io/community\n"
     ]
    }
   ],
   "source": [
    "# Close SQL magic connection\n",
    "%sql --close postgresql://127.0.0.1:5432/imdb"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "source": [
    "## Submission\n",
    "\n",
    "Make sure you have run all cells in your notebook in order before running the cell below, so that all images/graphs appear in the output. The cell below will generate a zip file for you to submit. **Please save before exporting!**\n",
    "\n",
    "After you have run the cell below and generated the zip file, you can download your PDF <a href='hw01.pdf' download>here</a>."
   ]
  },
  {
   "cell_type": "code",
   "execution_count": 243,
   "metadata": {
    "deletable": false,
    "editable": false
   },
   "outputs": [
    {
     "name": "stdout",
     "output_type": "stream",
     "text": [
      "Running your submission against local test cases...\n",
      "\n",
      "\n",
      "\n",
      "Your submission received the following results when run against available test cases:\n",
      "\n",
      "    q0 results: All test cases passed!\n",
      "\n",
      "    q1a results: All test cases passed!\n",
      "\n",
      "    q1b results: All test cases passed!\n",
      "\n",
      "    q1c results: All test cases passed!\n",
      "\n",
      "    q1d results: All test cases passed!\n",
      "\n",
      "    q2a results: All test cases passed!\n",
      "\n",
      "    q2b results: All test cases passed!\n",
      "\n",
      "    q2c results: All test cases passed!\n",
      "\n",
      "    q3a results: All test cases passed!\n",
      "\n",
      "    q3b results: All test cases passed!\n",
      "\n",
      "    q3c results: All test cases passed!\n",
      "\n",
      "    q4a results: All test cases passed!\n",
      "\n",
      "    q4b results: All test cases passed!\n",
      "\n",
      "    q5 results: All test cases passed!\n"
     ]
    },
    {
     "data": {
      "text/html": [
       "\n",
       "                    <p>\n",
       "                        Your submission has been exported. Click\n",
       "                        <a href=\"proj1_2023_09_14T19_54_44_927071.zip\" download=\"proj1_2023_09_14T19_54_44_927071.zip\" target=\"_blank\">here</a> to download\n",
       "                        the zip file.\n",
       "                    </p>\n",
       "                "
      ],
      "text/plain": [
       "<IPython.core.display.HTML object>"
      ]
     },
     "metadata": {},
     "output_type": "display_data"
    }
   ],
   "source": [
    "# Save your notebook first, then run this cell to export your submission.\n",
    "grader.export(pdf=False, run_tests=True, files=['proj1.pdf', 'results.zip'])"
   ]
  },
  {
   "cell_type": "markdown",
   "metadata": {},
   "source": [
    " "
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.11.0"
  },
  "otter": {
   "OK_FORMAT": true,
   "tests": {
    "q0": {
     "name": "q0",
     "points": 1,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_0, result_0_df = grading_util.load_results(\"result_0\")\n>>> result_0_df.shape[0] == 69\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_0, result_0_df = grading_util.load_results(\"result_0\")\n>>> result_0_df.iloc[0, 0] == 'information_schema_catalog_name'\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q1a": {
     "name": "q1a",
     "points": 1,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> len(result_1a) == 4\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> \"movie_id\" in result_1a\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q1b": {
     "name": "q1b",
     "points": 2,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> result_1b.DataFrame().shape[0] == 1\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> 2400000 <= count_1b <= 2450000\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q1c": {
     "name": "q1c",
     "points": 1,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> 0.0001 <= p_1c <= 0.001\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> assert (\"BERNOULLI\" in query_1c.upper()) == True\n",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q1d": {
     "name": "q1d",
     "points": 1,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_1d, result_1d_df = grading_util.load_results(\"result_1d\")\n>>> result_1d_df.shape[0] == 5\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_1d, result_1d_df = grading_util.load_results(\"result_1d\")\n>>> assert (\"RANDOM\" in query_1d.upper()) == True\n",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q2a": {
     "name": "q2a",
     "points": 1,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> mpaa_rating_id == 97\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q2b": {
     "name": "q2b",
     "points": 3,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_2b, result_2b_df = grading_util.load_results(\"result_2b\")\n>>> result_2b_df.shape == (1, 2)\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_2b, result_2b_df = grading_util.load_results(\"result_2b\")\n>>> result_2b_df['info'][0] == 'Rated PG-13 for sexual content, language and some teen partying'\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_2b, result_2b_df = grading_util.load_results(\"result_2b\")\n>>> result_2b_df['mpaa_rating'][0] == 'PG-13'\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q2c": {
     "name": "q2c",
     "points": 3,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_2c, result_2c_df = grading_util.load_results(\"result_2c\")\n>>> result_2c_df.shape[0] == 20\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_2c, result_2c_df = grading_util.load_results(\"result_2c\")\n>>> result_2c_df.shape[1] == 4\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_2c, result_2c_df = grading_util.load_results(\"result_2c\")\n>>> result_2c_df['movie_id'].iloc[:10].sum() == 16333796\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_2c, result_2c_df = grading_util.load_results(\"result_2c\")\n>>> 'mpaa_rating' in result_2c_df.columns\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_2c, result_2c_df = grading_util.load_results(\"result_2c\")\n>>> 'movie_id' in result_2c_df.columns\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q3a": {
     "name": "q3a",
     "points": 2,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_3a, result_3a_df = grading_util.load_results(\"result_3a\")\n>>> len(result_3a_df) == 10\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_3a, result_3a_df = grading_util.load_results(\"result_3a\")\n>>> result_3a_df.iloc[0, 2] == 'Avatar'\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_3a, result_3a_df = grading_util.load_results(\"result_3a\")\n>>> result_3a_df['gross'].iloc[:5].sum() == 3038331946.0\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q3b": {
     "name": "q3b",
     "points": 2,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_3b, result_3b_df = grading_util.load_results(\"result_3b\")\n>>> len(result_3b_df) == 10\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_3b, result_3b_df = grading_util.load_results(\"result_3b\")\n>>> result_3b_df.iloc[0, 2] == 'Pirates of the Caribbean: At World\\'s End'\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_3b, result_3b_df = grading_util.load_results(\"result_3b\")\n>>> result_3b_df['budget'].iloc[:5].sum() == 1318000000.0\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q3c": {
     "name": "q3c",
     "points": 2,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_3c, result_3c_df = grading_util.load_results(\"result_3c\")\n>>> len(result_3c_df) == 10\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_3c, result_3c_df = grading_util.load_results(\"result_3c\")\n>>> result_3c_df.iloc[4, 1] == 'The Avengers'\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_3c, result_3c_df = grading_util.load_results(\"result_3c\")\n>>> result_3c_df.iloc[4, 1] == 'The Avengers'\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_3c, result_3c_df = grading_util.load_results(\"result_3c\")\n>>> result_3c_df['profit'].iloc[:5].sum() == 2260084056.0\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q4a": {
     "name": "q4a",
     "points": 2,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_4a, result_4a_df = grading_util.load_results(\"result_4a\")\n>>> result_4a_df.shape[1] == 5\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_4a, result_4a_df = grading_util.load_results(\"result_4a\")\n>>> result_4a_df.iloc[0, 2] == 186336103.0\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q4b": {
     "name": "q4b",
     "points": 2,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_4b, result_4b_df = grading_util.load_results(\"result_4b\")\n>>> result_4b_df.iloc[0, 0] == 30.0\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_4b, result_4b_df = grading_util.load_results(\"result_4b\")\n>>> result_4b_df.iloc[0, 2] == 2317091.0\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    },
    "q5": {
     "name": "q5",
     "points": 2,
     "suites": [
      {
       "cases": [
        {
         "code": ">>> query_5, result_5_df = grading_util.load_results(\"result_5\")\n>>> result_5_df.iloc[3, 1] == 'Trebek, Alex'\nTrue",
         "hidden": false,
         "locked": false
        },
        {
         "code": ">>> query_5, result_5_df = grading_util.load_results(\"result_5\")\n>>> result_5_df['number'].iloc[:5].sum() == 26115\nTrue",
         "hidden": false,
         "locked": false
        }
       ],
       "scored": true,
       "setup": "",
       "teardown": "",
       "type": "doctest"
      }
     ]
    }
   }
  }
 },
 "nbformat": 4,
 "nbformat_minor": 4
}
