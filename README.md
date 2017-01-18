# Airflow
Repo for airflow DAGs and plugins

# Intro
Airflow works by defining workflows, which are called DAGs. It's called DAG because each workflow is a directed acyclic graph defining dependencies between different operators.

# Set up
Please install this on your EC2 dev server! We wish to cease running from our laptops so that we don't have student data on laptops.

* Clone this repository. It must be deployed to your home directory.

<pre>
    cd ~
    git clone git@github.com:FB-PLP/airflow.git
</pre>

* Install the airflow software using our script.

<pre>
    cd airflow
    sudo scripts/tmp_on_disk_ec2.sh
    sudo scripts/install_ec2.sh
</pre>

* You should now be able to run the <code>airflow</code> command.

* Do the tutorial: https://airflow.incubator.apache.org/tutorial.html. This will teach you how to use airflow.

* Write a DAG in the dags/redshift_east folder and figure out how to use <code>airflow test</code> to run it. Here's an example:

<pre>
    airflow test redshift_dim_courses_historical wait_for_courses 2016-12-12
</pre>

# Code review
We use phabricator for code review. Please use <code>arc diff</code> and add at least astewart as reviewer.

# Web interface
The web interface is hosted at http://ec2-107-21-30-86.compute-1.amazonaws.com/ to view job status.

That EC2 machine runs both the webserver and scheduler and will actually run your job. As long as your DAG is checked in, this machine will automatically start running your job.

# Note
* The database is currently shared between the EC2 instance and your machine. Please just run airflow test and not other commands that may modify the database.
