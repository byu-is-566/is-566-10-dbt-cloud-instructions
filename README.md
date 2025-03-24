# Hands-On Lab: DataOps with dbt Cloud

Welcome back! I'm sure you've been looking forward to this next assignment as much as I have. Well, it's here. In this _adventure_ (foreshadowing), we'll take our data pipeline skills to the next level, learning the ins and outs of the world's most elegant data modeling tool: dbt. Among the dozens of data tools that I've been exposed to over the course of my career, dbt is probably my favorite. It just _speaks_ to me. I hope it does to you, too.

---

## Overview of the Lab

Before jumping into the weeds, I'm going to provide a summary of the things you'll be doing and the overall goal of the assignment/tasks I'll be asking you to accomplish. 

In short, we're going to be building a portion of a data warehouse from scratch, starting with raw source data from two external "systems", then progressively transforming and improving that data in a series of warehouse tables that roughly follow the bronze, silver, and gold layers we've discussed in the past. (Of course, a complete warehouse build is too big a task for a single assignment, so we'll be doing enough to get a feel for it without going too far into the redundancies.) Here are the general steps you'll be following:
1. First (and before we even set up our dbt environment), we're going to load some "source" data into a dedicated schema that will represent the "external system" from which that data is supposedly coming. This will give us a chance to remind ourselves how to use Snowflake (!), and should be a nice warmup exercise.
2. Next, we'll ensure that you have dbt available and configured to run in your local environment, and we'll step through a sample model that I've created for you to use as a reference when you go to build out your own models.
3. Here's where the fun begins: we'll start with the simpler of two data pipelines that will pass data through the warehouse. These pipelines will start with the _bronze layer_ (comprised of "sources", "base models", and "seeds", to use the dbt terms). From these, we'll derive the _silver layer_ in the form of "staging" and "intermediate" tables, in which we'll do some light cleaning, renaming, and otherwise enriching the data to support a few downstream analyses. 
4. As if that wasn't enough, we'll then go through the process _again_, this time with some slightly more challenging semi-structured data. We'll take that data through a similar flow ("sources", "base models", then "staging" and "intermediate" tables). 

> "Okay, already. Let's start building." 

Not quite yet. Because there are _a lot_ of different files involved in building out a dbt repository, I'm going to give you a glimpse of what the final state of your assignment repository will be, along with a few annotations to indicate which part is which. (Depending on how you're viewing these instructions, you might consider opening this in a separate, wider window.)

```bash
is-566-09-dbt-1
├── analyses              
├── dbt_project.yml    
├── logs
├── macros
├── models
│   ├── intermediate    
│   │   ├── int_sales_order_line_items.sql       # << Part 4    
│   │   └── int_sales_orders_with_campaign.sql   # << Part 4    
│   └── staging  
│       ├── adventure_db    
│       │   ├── stg_adventure_db__customers.sql        # << Part 3    
│       │   ├── stg_adventure_db__inventory.sql    
│       │   ├── stg_adventure_db__product_vendors.sql  # << Part 3    
│       │   ├── stg_adventure_db__products.sql         # << Part 3    
│       │   └── stg_adventure_db__vendors.sql          # << Part 3    
│       ├── ecom   
│       │   ├── base  
│       │   │   ├── base_ecom__email_campaigns.sql  # << Part 4    
│       │   │   └── base_ecom__sales_orders.sql     # << Part 4    
│       │   ├── stg_ecom__email_campaigns.sql       # << Part 4    
│       │   ├── stg_ecom__purchase_orders.sql       # << Part 4    
│       │   └── stg_ecom__sales_orders.sql          # << Part 4    
│       └── sources.yml    
├── seeds
│   ├── _seeds.yml            # << Part 3
│   ├── measures.csv          # << Part 3
│   └── ship_method.csv       # << Part 3
├── snapshots                                                             
├── target                                                             
└── tests                                                             
```

Hopefully that was helpful? 🤷🏼‍♂️ I know that the hierarchy may feel a bit counterintuitive, but we're following the suggested "best practice" naming conventions and project structure from the folks at dbt Labs. It'll hopefully make sense as we progress through the project.

Before we head to our first Snowflake task, just a quick note on the naming convention used for the SQL files that define dbt models. You'll see above that, regardless of where the file sits in the folder structure, the model files all use the following format:

```
[layer]_[source]__[entity]s.sql
```

There are lots of reasons for the very specific naming approach (which you can read about [here](https://docs.getdbt.com/best-practices/how-we-structure/2-staging) if you're interested), but perhaps the most important is that since the file names _become_ the names of the tables/views created by dbt in the database, the naming conventions will conveniently group similar tables together when you browse the data. (This makes autocomplete suggestions extremely helpful as well.)

Okay. Enough context. Let's get started, shall we?

---

## Task 1: Snowflake Prep Work

It's time to dust off those Snowflake skills and get a few things set up in Snowflake. We've talked about all of the things you'll do for this task, but it's been a little while. I'm sure it'll come right back to you, just like riding a bike.

You'll be incorporating data from two hypothetical systems. One of these is an OLTP database that I've prepared for you within our classroom account (and we'll come back to that data in the next task). The other source of data is one that you'll be loading yourself in your own database (e.g., `EAGLE_DB`).  

> [!TIP]
> Did you know that I provided a full walkthrough and some example code back when we went over this in class? Well, I did. You might find that useful here. Check out the code found here: `is-566-00-in-class-activities/4 - Snowflake stages/commands.sql` and the accompanying walkthrough video [here](https://www.dropbox.com/scl/fi/o7rlfgyzythwo94587vuo/Snowflake-stages-and-file-formats.mov?rlkey=jdxojpxnvsc82bc5blo2zhyuk&dl=0).

Included here in the repository is a data folder with about 300MB of data, all in JSON format. We're going to load these data into three tables, all within a dedicated schema called `ECOM_SOURCE` (which you'll need to create). I've provided a bit of starter SQL code in `566/ecom_stage_and_load.sql` to get you started.

And just to be abundantly clear, this first SQL file has nothing to do with dbt. We just need to get some data up into your individual databases, and rather than loading it for you and having you copy it, I thought it would be good experience for you to do what a data engineer would have to do: load the data.

> [!IMPORTANT]
> Some of you will be tempted to do the SQL work below in the Snowflake browser interface. This is the least convenient way to go about this, especially because the `PUT` commands won't work in the browser interface. Your best bet is to just use the Snowflake plugin for VS Code or the `snowsql` command line utility that we configured together. If you can't get either of those to work, you can manually upload the files by navigating to the stage you're about to create and then clicking on various buttons to upload the files to the stage. But you'll lose _street cred_ because you're using a mouse cursor in the GUI. 

Use `ecom_stage_and_load.sql` to:
1. Write 3 table creation commands to create `purchase_order_raw`, `sales_order_raw`, and `email_campaign_raw`. Each table will have just one `RAW` column to house the semi-structured data. Any guesses what datatype that column will be? Let's just say that it'll be _burdened with glorious purpose_.
2. Create a Snowflake `FILE FORMAT` to be used with all of the JSON files we're going to upload. The file format can be extremely generic (meaning that you mostly just have to specify that these will be JSON files), but I'll give one other hint that will hopefully save you some troubleshooting: have a look at the `STRIP_OUTER_ARRAY` option for the file format so that Snowflake will recognize the individual JSON objects as separate records (instead of assuming that each _file_ is one big record because of the square brackets at the beginning and ending of the files).
3. Create an internal stage, making sure to include your new file format as a default file format for the stage. Then you can use a few `PUT` commands to upload the JSON files located in `566/data/ecom_source_data`. For `purchase_orders.json` and `sales_orders.json`, you can just upload them. But for all of the JSON files in the `email_campaign` subfolder, I would suggest that you put them in their own subdirectory in the stage. In other words, you'll use something like: `PUT 'file:///path/to/email_campaign/*.json' @your_stage/email_campaign`. (For Windows users, the same command would be: `PUT 'file://C:\\path\\to\\email_campaign\\*.json' @your_stage/email_campaign`.) This will make it much easer in the next step to just load all of the email data with a single command because you can use `FROM @your_stage/email_campaign/` instead of naming the files individually. Neat-o, right?
4. Lastly, use 3 `COPY INTO` commands to populate the three tables you created in step 1 above. I'd suggest that you check to make sure that you can select a row from each table with a quick `select * from email_campaign_raw limit 1;`, which should return something that looks like my screenshot below.

<img src="566/screenshots/readme_img/raw_load.png"  width="80%">

> [!IMPORTANT]
> 📷 Grab a screenshot of your own query result similar to mine above. Save this screenshot as `task_1.png` (or jpg) to the `566/screenshots` folder in the assignment repository.

---

## Task 2: Your First dbt Command

Next, let's make sure you can run dbt and that you have the project configured to connect to your own Snowflake assets. 

### 2.1 - Validate dbt Connection to Snowflake

The steps below are a summary of how to ensure that your dbt configuration can talk with Snowflake. (If you haven't watched my [video walkthrough](https://www.loom.com/share/31f3470f5d5f4cc6a3fbcdaee3951012?sid=95abb829-3c70-4c6f-b077-d7c85a7f8467) of how to get dbt installed and running, I would start there.)

Just to give you a checklist beyond what I walk through in the video linked above, here's the things you can do:
1. Open a terminal in your assignment repository folder.
2. Activate (or create if necessary) your dedicated `dbt` conda environment. (If you're creating your conda environment, just install `python=3.12`, `dbt-core` and `dbt-snowflake`.)
3. Ensure that you have a profile in your `.dbt/profiles.yml` that matches the `profile:` line in the `dbt_project.yml` file in the assignment folder. **The profile should be configured to use a schema called `dbt` in your own `USERNAME_db` database**. (See my example profiles.yml file below.)
4. Run `dbt debug` in the assignment folder, and verify that you can connect successfully.

Here's what your `.dbt/profiles.yml` file should look like:
```yaml
# For the project to work properly, you should have something very similar to 
# this in your .dbt/profiles.yml

adventure:
  outputs:
    dev:
      account: Your-account    # << change this                    
      database: USERNAME_DB    # << change this                
      password: your_password  # << change this                  
      role: USERNAME_ROLE      # << change this              
      schema: dbt             
      threads: 1         
      type: snowflake    
      user: YOURUSERNAME       # << change this             
      warehouse: USERNAME_WH   # << change this                 
  target: dev
```

(Again, if any of the above doesn't make sense, check out the video I linked above for a detailed walkthrough of how dbt configuration works.)

---

### 2.2 - Run the Existing Model

Assuming your `dbt debug` command was successful, you should be able to just run `dbt build` without making any other changes. If all goes well, you'll see the `stg_adventure_db__inventory` model run successfully, and you'll likely see a nice green "SUCCESS" message in your terminal like my screenshot below.

<img src="566/screenshots/readme_img/first_dbt.png"  width="80%">

Note that you can always check your Snowflake account to see the effects of these model runs as well. Depending on configuration, each "success" message will result in a new or updated table or view. (You don't need to do that for this task.)

> [!IMPORTANT]
> 📷 Grab a screenshot of your own successful `dbt build`, similar to mine above. Save this screenshot as `task_2.2.png` (or jpg) to the `566/screenshots` folder in the assignment repository.

---

## Task 3: The Adventure DB Pipeline

In this task, we'll build out a multi-layered data pipeline using data from a standard relational database used by our fake company (AdventureWorks). The full database (with a total of 5 tables) is available in the shared `IS566` database in the `adventure_prod_db` schema. (We're going to pretend that the tables in that schema are in an external system that we have to connect to in order to pull in the transactional data.)

---

### 3.1 - Configure `adventure_db` Sources and Seeds

First, let's establish the origin of the `adventure_db` pipeline. This is primarily found in the `IS566.adventure_prod_db` tables as mentioned, and the mechanism dbt uses to access "external" data is called a _source_. Have a look at the configuration found in `models/staging/sources.yml`. (If you need help interpreting the various options there, you can consult [the documentation](https://docs.getdbt.com/reference/source-properties), but don't get lost in all the options; we'll only really use the ones you already see in the current `sources.yml` file.)

Your first task is to replicate the configuration you see for the `inventory_prod_db` source to add the other 4 tables as similar sources. Note that you can't really directly test whether your source configuration is correct (because sources are accessed on demand when downstream models query them), but we'll verify the configuration in the next task.

While we're here on the outskirts of the _bronze_ layer of the warehouse, though, we're also going to configure a couple of _seed_ files that we can reference as needed as we refine the raw data. (Seeds and sources often get mixed up, and I think [this short article](https://mbvyn.substack.com/p/understanding-dbt-seeds-and-sources) provides a really nice explanation of the distinction.) 

The seed files I've included (see `566/data/seed_files/`) are very small "labeling helpers" that we can use to help us refine some column names in a minute. Sometimes we use little files like this as a convenience (or security) strategy when the reference material being stored is too trivial to justify a separate database/schema. dbt allows us to store small files like this right in the repository, and it takes care of creating a little reference table alongside the other dbt-managed resources in the warehouse so can use the reference data as needed.

Move the files from `566/data/seed_files/` to their rightful place in the `seeds/` folder. Now that the files are in a directory identified in the `seed-paths` attribute in the `dbt_project.yml`, dbt will generate and maintain a table for each of the files. dbt is actually pretty smart about guessing the right structure and datatypes for those tables, but it's also possible to explicitly configure the seeds with a few features. Rather than make you go learn about [obscure seed configuration options](https://docs.getdbt.com/reference/seed-configs), though, I'll just give you the contents of a file you can add to the `seeds/` folder. This file can be called whatever you'd like, but by convetion, it's called `_seeds.yml`. (The underscore is so that it always sorts at the top of the directory.)

```yaml
version: 2

seeds:
  - name: measures
    column_types: 
      measure_code: STRING
      measure_name: STRING

  - name: ship_method
    column_types:
      ship_method_id: INTEGER
      name: STRING
      ship_base: NUMBER
      ship_rate: NUMBER
```

Now that your seeds are in and configured, you can run `dbt seed` (which will process and insert only the seed files) or `dbt build` (which will do the seed command along with everything else). You should see output like you see in my screenshot below. 

<img src="566/screenshots/readme_img/seed_run.png"  width="80%">

> [!IMPORTANT]
> 📷 Take a screenshot of your own successful `dbt seed` or `dbt build`, similar to mine above. Save this screenshot as `task_3.1.png` (or jpg) to the `566/screenshots` folder in the assignment repository.

---

### 3.2 - Add `adventure_db` Staging Tables

With the sources and seeds setup for the adventure database, we can now create the rest of the staging tables for the `adventure_db` pipeline. (And I'm going to suggest a **specific order** to follow so that you start with the easier ones.) 

Using the existing `stg_adventure_db__inventory` model as a template, you can now create staging tables for the 4 remaining tables in the `IS566.adventure_prod_db` schema.

> [!TIP] 
> I debated how to best guide you through creating these models because I don't want to just leave you to guess on column names, etc., but I also don't want to just give you all the code because I think you'll learn more from having to write the queries yourself. Of course, as you can see in the example inventory model, these models are mostly just passing the data through from the source, so the complexity is very low.
>
> Ultimately, I landed on trying to strike a balance. So for each of the models you'll be building for the rest of the assignment, I'll ask you for the model, provide some requirements, and then I'll highlight or hint about anything that I think might be tricky. If, however, I err on the side of too-vague, please just reach out and ask. I'll fill in any holes that are needed.

Okay! So let's build these four staging models, in the order I present them below, and accounting for the small nuances I'll highlight.

#### `stg_adventure_db__vendors` and `stg_adventure_db__customers`

- These will both follow a similar (and simple) approach.
- Remember that you can copy the structure of the existing inventory model
- Be sure to follow the "snake_case" naming convention we're using everywhere else
- That's actually it...these two are very straightforward.

#### `stg_adventure_db__product_vendors`

- This one is mostly the same as the previous two, with one exception/complexity. 
- This table should use our handy measures seed file to replace the `UnitMeasureCode` with the actual name of the measure. You can call the resulting column whatever makes you happy. (I called mine `measurement`.)
- Remember that everything is easier when you use CTEs...

#### `stg_adventure_db__products`

- Again, similar to the previous one, but this time there are two measurement codes to replace with measurement names.
- The hardest part of this one is figuring out how to join the measures seed data in _twice_. 
- FWIW, I called the resulting columns `size_measure` and `weight_measure`.

Wasn't that fun? I thought so, too. If all went well, you should be able to run your dbt build process again, and you'll now see a total of 5 `stg_*` views being created, along with the seeds, etc., similar to my screenshot below.

<img src="566/screenshots/readme_img/adv_stg_run.png"  width="80%">

> [!IMPORTANT]
> 📷 Take a screenshot of your own successful `dbt build`, similar to mine above. Save this screenshot as `task_3.2.png` (or jpg) to the `566/screenshots` folder in the assignment repository.

---

### 3.3 - Adjust Materialization Settings

Before we move on to the second pipeline, just a quick configuration change that will help you verify that your work matches mine from here on out. 

You may have noticed that just 2 of the 7 "SUCCESS" messages in the output above have metadata about rows being inserted into the database, with the rest defaulting to views. There's nothing wrong with views, but in my experience they are generally reserved for the later stages of a data warehouse pipeline. Somewhere in the middle of the warehouse pipeline, we should reach a point where the data "stabilizes" in a sense, meaning that we've cleaned up, relabeled, and otherwise refined the data from a given source system such that it's ready to be stored in the warehouse for long term accumulation and use. It usually makes sense for these models to become tables, and we can assume that our `adventure_db` tables fit those criteria.

With that (unsolicited, I know) background, let's make a change to the dbt configuration to ensure that all models leading up to the staging tables are _materialized_ as tables, while those in the intermediate layer remain views.

There are several ways to accomplish this, but probably the simplest for the scenario above would be to use the model configuration section near the bottom of the `dbt_project.yml` to set the `materialized` attribute differently for the staging vs. intermediate hierarchies. 

The syntax for this is a bit funky, and the way that dbt interprets the project hierarchy is a bit counterintuitive. But because you want to impress your manager some day with your ability to read documentation and figure something out, I'm going to point you to [this particular section](https://docs.getdbt.com/reference/resource-configs/resource-path#apply-config-to-all-models-in-a-subdirectory) of the documentation on how to set configuration options based on the "resource path" within the project, and then leave you to experiment a bit with some trial and error.

What you're looking to see in your output is what you see in my screenshot below. Pay particular attention to the yellow "WARNING" message you see in my output, which is something you should expect as well. That warning message indicates that we have properly applied a configuration option to a level of the hierarchy that currently doesn't have any models, which is true because we haven't added any intermediate tables just yet. (Stay tuned.) We can verify that those intermediate configuration options work as expected after we create those models, but for now, a close look at your output will reveal that all 7 of the models being run are creating tables now. 

<img src="566/screenshots/readme_img/switch_to_tables.png"  width="80%">

Also, if you glance at the `dbt` schema in your database, you'll see those new tables, but you'll likely also see those views still hanging around. This is because dbt intentionally does not support deleting of old tables or views as part of the build process. You can imagine that the risk of that having unintended consequences is pretty high. So dbt only affects the objects that are still defined as models in the project (meaning that it will happily replace the data in a table or view that it is managing each time it runs, but renamed or abandoned objects will just stay out there unless you manually drop them).

When your data is small and you're in active, iterative development, the easiest way to get rid of the fluff is to just drop the whole `dbt` schema. I personally just keep an SQL file open in my environment with a few such commands so I can use them to "reset" as needed.

Okay. Enough of that. Let's do a screenshot, shall we?

> [!IMPORTANT]
> 📷 Take a screenshot of your own successful `dbt build`, similar to mine above. Save this screenshot as `task_3.3.png` (or jpg) to the `566/screenshots` folder in the assignment repository.

---

## Task 4: The (More Fun) `ecom` Pipeline

I'm sure you've heard the saying that data pipelines are better in pairs? No? Well. Now you have. This second pipeline is just a bit more complex, but the basic flow of data is largely the same. Aside from it being stored in semi-structured format, there are also a few nuances related to messiness that _somehow_ made their way into the datasets. Weird, right? It's okay because I'll point you straight to them.

### 4.1 - Configure `ecom` Sources and Base Tables

The data we uploaded in Task 1 should still be waiting patiently in the `ECOM_SOURCE` schema. The first task is to expand the configuration in the `sources.yml` file to add a second named source. When you make these changes, be sure that you're accounting for the fact that the two schemas are in different databases (and should be configured with different source names). In the end, you should have a total of 8 source tables identified in `sources.yml`. (Again, we can't really test them at this point, but we'll get to that.)

Next, let's talk about **base tables**. Base tables constitute a sort of "loading dock" for the warehouse (if you'll allow me to stretch the analogy a bit). More often than not, we'll bring data straight from a source table to a staging table, transforming or renaming the columns as a part of the query logic. Sometimes, however, it can be simpler to "stash" the data in a base table to use for comparison or other operations, particularly when the data coming from the source is hard to work with. 

The scenario I've created with this `ecom` data may not fit these criteria exactly, but I wanted to expose you to the concept anyway, so here we are. The idea is that we'll be creating two base tables, one for the sales order data, and another for the email campaign data. Your goal when creating these base tables is to extract them from their respective source tables without really changing much aside from (a) changing the column naming convention to snake_case (if needed), and (b) applying data formatting to ensure that the data coming out of the semi-structured fields is formatted as dates and numbers (with proper precision when necessary). Below are the descriptions of the 2 base tables you need to create, along with nuances and data cleanup needs that you should watch out for.

#### `base_ecom__email_campaigns`

- This is mostly just an extraction of each of the embedded fields and storing them with proper name and data formatting.
- Watch out for the date format - it'll require some conversion.

#### `base_ecom__sales_orders`

- This is again just an extraction of each of the embedded fields, but pay attention to the column names and other formatting. 
- In particular, make sure that you don't lose precision on any of the numbers you're extracting.
- Lastly, this base table should retain the embedded order detail information as VARIANT column (i.e., the table should remain a sales-order-level table)

Okay, with those base tables configured, we can do another `dbt build` to see the effect of our changes. Here's a screenshot of what my build process output looked like:

<img src="566/screenshots/readme_img/base_built.png"  width="80%">


> [!IMPORTANT]
> 📷 Take a screenshot of your own successful `dbt build`, similar to mine above. Save this screenshot as `task_4.1.png` (or jpg) to the `566/screenshots` folder in the assignment repository.

---

### 4.2 - Add `ecom` Staging Tables

These last three staging models will be similar to those in the adventure_db, with a few additional considerations. We'll look at them from easiest to most difficult:

#### `stg_ecom__purchase_orders`

- After doing the base table models with the semi-structured columns, this one actually shouldn't be very difficult. 
- As with the `base_ecom__sales_orders` model, be sure that you aren't losing any precision in numbers that get extracted.
- You'll need to join in data from the ship_methods seed file to extract the name (rather than the id) of the shipping method. 
- Lastly, you should ensure that each of the nested items under the order details column are simply brought out to the top level as columns in the model, and that the resulting columns also use the snake_case conventions.

#### `stg_ecom__sales_orders`

- This stage table should read from the sales orders base table (which already has formatting applied). 
- You'll need to join in data from the ship_methods seed file to extract the name (rather than the id) of the shipping method. 
- Lastly, look carefully at the values in `delivery_estimate` column. These messy data need to be cleaned up before the data can move further downstream. The goal is to replace that column with a numeric column called `delivery_estimate_days` that measures the delivery estimates in a single unit of measurement. You're welcome to just use a basic CASE statement to match the existing set of values, but you could also have some fun thinking about how to match a little more flexibly with some regular expressions. (No pressure.)

#### `stg_ecom__email_campaigns`

- This data has also already been extracted and formatted with datatypes in the base table, so most of the columns can just be passed through. 
- You will also turn the `campaign_id` column into 4 total columns that represent the different elements embedded in the `campaign_id`, namely `customer_segment`, `product_category`, and `ad_strategy` (and retain the `campaign_id` column).
- You also should add a new binary flag column called `is_converted` that uses the presence or absence of an order_id to fill in the column with a 1 or a 0. 
- You may not have noticed this yet, there are actually duplicate records in the email campaigns base table (The event_id should be a unique primary key in that table.) So your query for this staging table should take care of de-duplicating the data (keeping the most recent event in the case where there are more than one even with the same event_id). I would recommend doing this in a CTE called "deduped", and I would also look into how the `QUALIFY` command works.

Okay this is exciting! We're up to 12 different tables that dbt is managing for us, and if you've got everything up and running, you'll have another terminal window full of green messages like below:

<img src="566/screenshots/readme_img/ecom_stg.png"  width="80%">


> [!IMPORTANT]
> 📷 Take a screenshot of your own successful `dbt build`, similar to mine above. Save this screenshot as `task_4.2.png` (or jpg) to the `566/screenshots` folder in the assignment repository.

---

### 4.3 - Add `ecom` Intermediate Tables

The last models we'll add will be closer to the business use cases related to these datasets. And I couldn't very well let you out of this assignment without at least one `LATERAL FLATTEN`, now could I?

#### `int_sales_order_line_items`

- This first one assumes we have an analysis use case for the order details of the items we've sold in the sales_order table. So this model will contain just a few sales_order columns (specifically, `sales_order_id`, `customer_id`, and  `order_date`), and then the order detail as a fully expanded and pivoted dataset.
- To keep things relatively simple, there is nothing else fancy about this model. Just flatten the order details into their respective columns and call it a day. 
- (Watch out for number precision again.)

#### `int_sales_orders_with_campaign`

- The scenario for this model is that the business would like to understand the sales activities of various product categories as they relate to some email marketing campaigns the company has been running. So the goal is to simply connect the sales data (i.e., `stg_ecom__sales_orders`) with the campaign data (i.e., `stg_ecom__email_campaigns`).
- Because I'm the one who made up the email campaign data, I'll just tell you: the campaign data has exactly one row for each order_id where the `event_type` is "conversion". Filtering to just those event types would be a nice way to get a joinable set of campaign data (you should join on order_id).
- The sales order data (which is larger than the campaign data) should form the basis for this dataset. \*Sniff, sniff\*. I smell a left join.
- You can include all of the columns from the sales_orders model, as well as the 4 campaign label columns (i.e., `campaign_id`, `customer_segment`, `product_category`, `ad_strategy`).
- You should also add a binary `is_campaign_conversion` column that is populated conditionally based on whether there is campaign data available for a given order_id.

We could keep going with lots of other intermediate models. But I think we'll hold it there for now. Doing a `dbt build` will allow us to check 2 things: (1) that our configuration setting from Task 3.3 is being properly applied to the intermediate models (meaning that they are being created as views; see the last two of the 14), and (2) that there's nothing but green success all over that terminal. Screenshot for reference:

<img src="566/screenshots/readme_img/int_run.png"  width="80%">


> [!IMPORTANT]
> 📷 Take a screenshot of your own successful `dbt build`, similar to mine above. Save this screenshot as `task_4.3.png` (or jpg) to the `566/screenshots` folder in the assignment repository.


---

## Submission Instructions

Ensure you have completed all tasks and saved the following screenshots:
- `task_1.png`
- `task_2.2.png`
- `task_3.1.png`
- `task_3.2.png`
- `task_3.3.png`
- `task_4.1.png`
- `task_4.2.png`
- `task_4.3.png`

Commit all code and screenshots to your repository and push your changes. 
