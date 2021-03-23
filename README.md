# CloudComputingAssignment
# Project B- Batch Data Processing
# Author: Victor-Florian Davidescu
# SID: 1705734


# Created two versions of the Spark Advanced Word Count.

# Version 1 (Local) - SparkAdvancedWordCount-LOCAL.py

  Description: This version was created to test the functionality on a local machine or on a VM with Spark installed on it.
  
  # Tested environment details:
  - Machine type: Oracle VM Virtual Box
  - OS: Ubuntu 20.04
  - Packages installed: JDK, Scala, Git, Python 3.x, Apache Spark 3.0.2 with Hadoop 3.2 and later
    WARNING: It requires to configure the spark environment. 
    Details to how to install apache spark on Ubuntu can be found at this link: https://phoenixnap.com/kb/install-spark-on-ubuntu  
  - Used two VMs, one for the master server, the other one as worker
  - IMPORTANT! All VMs must have the same directory tree with the samples files included, to make the script work

  # How to run the script:
  - Command example: python3 SparkAdvancedWordCount-LOCAL.py spark://vm-master:7077 bucket/input bucket/output sample-a.txt
  - Argument 1 (spark://vm-master:7077): The URL link to the apache spark master server. 
  - Argument 2 (bucket/input): The directory path where the sample txt file is located.
  - Argument 3 (bucket/output): The directory path where the output sample txt file will be created.
  - Argument 4 (sample-a.txt): The name of the sample txt file.


# Version 2 (AWS-EMR) - SparkAdvancedWordCount-AWS-EMR.py

  Description: This version was created to test the functionality on a AWS EMR cluster. To run this script requires an AWS EMR and S3 bucket.
  
  # Tested EMR cluster details:
  - Machine type: Amazon Web Services (AWS) - Elastic MapReduce (EMR)
  - Release label: emr-6.2.0
  - Applications: Spark 3.0.1, Zeppelin 0.9.0
  - Hardware used for master and core: m5.xlarge (Had to use this version, since my account's region doesn't support cheaper versions)
  - Add AmazonS3FullAccess and AmazonS3OutpostsFullAccess to EMR_EC2_DefaultRole.
  - Add AmazonS3FullAccess and AmazonS3OutpostsFullAccess to EMR_DefaultRole.
  - The command "aws emr mv <local_file> <s3://path/file>" is working.
  - Optional: Open ports for SSH for master, by adding it in its security group.
  - Optional: Open ports for SSH for slave, by adding it in its security group.

  # Tested S3 bucket details:
  - Simple S3 bucket with default options.
  - Added pythno script SparkAdvancedWordCount-AWS-EMR.py.
  - Added directory for the sample txt files and added the txt samples aswell.
  - Added directory for the output sample txt files.

  # How to run the script:
  - Create a cluster that has the requirements specified in "Tested cluster details"
  - Create an S3 bucket that has the requirements specified in "Tested S3 bucket details"
  - On EMR cluster create a new step and complete the following fields:
      > Step type: Spark application
      > Name: <any_name>
      > Deploy mode: Cluster
      > Spark-submit options: Empty
      > Application location: s3://<bucket_name>/<path_to_python_script>
      > Arguments: s3://<bucket_name> <directory_name_for_sample_txt_files> <directory_name_for_output_sample_txt_files> <sample_file_name>
      > Action on failure: Continue
  - Example of arguments passed: s3://victor1705734 input_samples output_samples sample-a.txt
  - Check the S3 bucket at the location you specified for the output sample txt file, to see the results.
