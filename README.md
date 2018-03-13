# DNAE - DoubleClick Network Analysis Enabler

A data integration framework built on top of DoubleClick APIs and Google Cloud
Platform.

## OVERVIEW

DNAE implements an ETL-like framework that can extract data from the DoubleClick
Digital Marketing platforms (DBM, DCM, DS), transform it as necessary and load
transformed data onto Google Cloud Storage and Big Query.
Taking advantage of the built-in BigQuery connector, Google DataStudio can be
used as visualization tool.
The framework is modular and can implement multiple "services" to provide
different kind of ETL flows and data insights.

Please note that this is not an officially supported Google product.

## INITIAL SETUP OF A DNAE PROJECT

Note: the following steps illustrate how to set up your DNAE project using the
included setup scripts. Feel free to customize your setup installing the
necessary files manually.

*   Download all the files in this folder on a local machine
*   From the command line, install the needed external libraries:

    -   Locally (you might consider using a [Python Virtual
        Environment](http://docs.python-guide.org/en/latest/dev/virtualenvs/)):

    ```shell
    pip install -r requirements.txt
    ```

    -   for AppEngine (downloading the relevant files in the third_party
        folder):

    ```shell
    pip install -t third_party -r requirements.txt
    ```

*   Setup your Google Cloud project:

    -   Make sure you have a Google Cloud project ready to use, or create one
        (please refer to the [Google Cloud
        documentation](https://cloud.google.com) for any additional information
        on how to create a Project).
    -   Install the [Google Cloud SDK](https://cloud.google.com/sdk/downloads)
        if you haven’t already.
    -   Check that your “default credentials” with gcloud correspond to the
        Google Account of the Google Cloud Project you want to create/use:

        ```shell
        gcloud auth application-default login
        ```

*   Run the script to setup the DNAE project, and follow the instructions:

    ```shell
    python dna_project_setup.py
    ```

    -   Check if you have write access to the files before running the script
    -   The interactive script will let you select the Google Cloud Platform
        project, give you instructions on which APIs to enable, guide you to the
        setup of the needed Credentials and update the template files with the
        IDs and the access details of your specific implementation
    -   DNAE uses v2 of the Task Queue REST API (now called Cloud Task API).
        This version is currently in "alpha" and you might need to have your
        account whitelisted in order to use the API (search the Cloud
        documentation for the latest status).
    -   If something goes wrong, you can run the followins script to restore the
        files to previous backup:

        ```shell
        python dna_restore_backup.py
        ```

*   DNAE (minus your specific _service_) is ready! Deploy the files to App
    Engine:

    ```shell
    ./deploy.sh
    ```

    -   To check that everything is OK, go to the Google Cloud Platform
        console > App Engine > Versions and you should see your “v1” app
        correctly serving
    -   you should also see a few “Cron Jobs” in App Engine > Cron jobs and
        they should run successfully if you start them manually
    -   Last but not least you should see your source files and scripts in the
        correspondent Cloud Storage buckets

*   You will now need to build your own _service_ to add to the DNAE framework.

    -   You can start running the following command to create the service folder
        and the main files you need (starting from the template files in
        “services/service-template”):

        ```shell
        python dna_service_setup.py
        ```

    -   Have a look at the sample service in folder “services/service-example”
        to see how you can interact with the DoubleClick APIs through the
        connectors included in DNAE, how to get the configuration data from an
        external Spreadsheet, how to elaborate the data before pushing it to Big
        Query and so on..

*   This setup creates 5 default cron jobs (in App Engine > Cron Jobs), all
    handled through methods in `lib/core/dna_gae_handlers.py`:

    -   `/core/cron/compute` which is the actual "task manager" job checking the
        Cloud Tasks queue for new tasks, for each of which starts a new Compute
        Engine VM instance
    -   `/core/cron/check/bqjobs` which updates any DataStore entry with
        `bqstatus`, `bqjob` and `bqerror` fields with the latest status of the
        corresponding BigQuery job
    -   `/core/cron/cleanup/compute` which deletes the Compute Engine VM
        instances which have completed their job
    -   `/core/cron/cleanup/datastore` which removes DataStore entities
        (typically every night, before a new run of the whole process)
    -   `/core/cron/cleanup/storage` which deletes files from Cloud Storage if
        older than a predefined number of days. In particular, if you want to
        use this cleanup job you need to create a new Entity in Datastore with
        kind name `DNACleanUpGCS` and with the following properties (case
        sensitive):
        -   `bucket`
            -   type: String
            -   value: my-cloud-storage-bucket
        -   `lbw`
            -   type: integer
            -   value: an integer representing a number of days for your Look
                Back Window (i.e. the number of days after which report files
                are removed from GCS)

## More info about a DNAE service

A typical DNAE-based service folder will include:

*   a _settings_ file, e.g. `service_example_settings.py` (in fact, see the
    files in folder `service-example` as reference)

    -   Keep in mind to:
        -   Create the GCS buckets you’re referencing in your settings (like
            `project-name-service-example`)
        -   Create the BQ datasets you’re referencing in your settings (e.g.
            `service_example_tables`) using “unspecified” as location)
    -   This is also where you’ll define the report/query params structure (json
        objects following the different API specifications - e.g. [DBM’s query
        resource](https://developers.google.com/bid-manager/v1/queries#resource)
        which can be tested via the [API
        Explorer](https://developers.google.com/apis-explorer/#p/doubleclickbidmanager/v1/doubleclickbidmanager.queries.createquery),
        BigQuery data schemas and other service-specific structures.

*   The main script, `service_example_run.py` with the actual steps to import
    and manage the data. This is obviously the most customizable (and complex!)
    part, where you’ll need to implement the import from the different data
    sources, the upload to the GCS bucket(s), the eventual transformation of the
    data, and the upload to the BigQuery dataset.

*   A test file for your service, e.g. `service_example_test.py` which will
    basically launch locally the same tasks that will be handled via the Cron
    Jobs and Cloud Tasks on GCP. In particular you’ll gather all relevant
    inputs/parameters (possibly a reduced set to make the test quicker) and then
    call the main function from `service_example_run.py` passing those
    parameters - just like you will do when creating the _handler_ function for
    GAE calls (see below).

*   The initial shell script file (e.g. `service-example-run.sh`, which calls
    the main python script (with two parameters for _queue name_ and _task id_
    matching the arguments expected by the _main_ method of
    `service_example_run.py`)

*   The file handling the requests coming from the AppEngine cron jobs, e.g.
    `service_example_gae_handlers.py`:

    -   You need at least one main handler (e.g. `ServiceExampleLauncher`),
        which is referenced in `appengine_main.py` as the method handling the
        calls to your service (calls coming from the scheduled cron jobs in
        `cron.yaml`). This handler, for each iteration (e.g. for each row of
        your configuration sheet) sets up a new `task_params` object with all
        needed parameters, such as service name, region, reference to the
        initial shell script (`service-example-run.sh`), bucket, dataset..
        anything needed by your service! This object is packaged into a payload,
        and the new task is added to the Cloud Tasks queue:

        ```python
        q.add(taskqueue.Task(payload=payload_str, method='PULL'))
        ```

## DNAE libraries and folders

The standard DNAE setup has:

*   A `lib` folder, which has

    -   a `connectors` folder, which includes all the libraries to “wrap” API
        functionalities for DBM, DCM, DS, Google Cloud Platform and Google
        Sheets
    -   a `core` folder, which includes the main files which will be copied into
        the Compute Engine virtual machine executing each task
    -   a `utils` folder, including different utility libraries

*   A `third_party` folder, populated when running the `pip install -t
    third_party -r requirements.txt` command, which includes the external
    libraries needed.

*   A `services` folder, one for each _service_ running in the project and with
    the corresponding files.
