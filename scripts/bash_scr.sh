#!/usr/bin/env bash
#Spark commands
export SPARK_HOME=/usr/hdp/current/spark2-client;
export SPARK_CONF_DIR=/usr/hdp/current/spark2-client/msla_conf;
export SPARK_MAJOR_VERSION=2;

#Control-M Input variables

appid=${1}
Odate=${2}
env=${3}

#File path of python scripts
folderpath="/data/${env}/pipelines/transform/${appid}/python/src/main/python/tasks"

hdfs_jar_path=hdfs:///${env}/jars

app_dir="/data/${env}/pipelines/transform/${appid}"

platform="${FRAMEWORK_PLATFORM:-classic}"
.${app_dir}/config/resources/env/${platform}/env-${env}.properties

echo "KERBEROS_KEYTABS=${KERBEROS_KEYTABS}"
echo"KERBEROS_PRINCIPAL=${KERBEROS_PRINCIPAL}"
kinit -kt ${KERBEROS_KEYTABS} -p ${KERBEROS_PRINCIPAL}


logpath="/data/${env}/logs/apps/${appid}"
timestamp=$(date+%Y%m%d-%H%M%S)
LOG_FILE=${logpath}'/'${appid}'_habse_to_hivedaily'${timestamp}'.log'

deploymode="cluster"
numerOfExecutotorCores="4"
numberOfExecutors="10"
executorMemory="16G"
driverMemory="8G"
driverCoreNumbers="2"

(
#HEADER
echo "--------------------------------------------------------------------------------------------"
echo "|                TITLE:RUN SPARK JOB                                                       |"
echo "--------------------------------------------------------------------------------------------"
echo ""
echo "                      1.APP_ID: " ${appid}
echo "                      2.ODATE: "  ${Odate}
echo "                      2.ENV: "  ${env}
echo "--------------------------------------------------------------------------------------------"
echo""


spark-submit --conf "spark.yarn.maxAppAttempts=1" --conf "spark.aunthentic.secret=none" \
            --master yarn \
            --deploy-mode $deploymode \
            --num-executors $numberOfExecutors \
            --executor-cores $numerOfExecutotorCores \
            --executor-memory $executorMemory \
            --driver-memory $driverMemory \
            --driver-cores $driverCoreNumbers \
            --conf spark.ui.port=4446 \
            --files /usr/hdp/current/hive-client/conf/hive-site.xml \
            --jars ${hdfs_jar_path}/spark-xml_2.11-0.8.0.jar \
            --py-files ${folderpath}/your_python_files.py,
            actual_python_files.py ${appid} ${Odate} ${env}
)2>&1 | tee ${LOG_FILE}

status=${PIPESTATUS[0]}

#Capture Spark Log
# shellcheck disable=SC1073
if [ -f "$LOG_FILE" ];
then echo "creating yarn logs.........locating Application ID"
#Defining yarn search element
  string='INFO\s\YarnClientImpl:\s\Submitted\s\application'

#Search for yarn application id in newly created log file
  yarn_id=$(grep -m 1 ${string} ${LOG_FILE} | awk '{print $NF}')
  echo "Application Id Found! YARN ID:" ${yarn_id}
#Directory path to store the yarn log
  yarn_log_path=${logpath}'/'${appid}'_habse_to_hive_daily_'${timestamp}'_'${yarn_id}'_yarn.log'
#Creating yarn logs
  yarn logs -applicationId ${yarn_id} > ${yarn_log_path}
  echo "Yarn log availble at : "${yarn_log_path}
else
  echo "Unable to create yarn logs,unable to locate " ${LOG_FILE}
fi

if [ -f ${LOG_FILE} ];
then
  echo "Log created successfully at, log file availble at:" ${LOG_FILE}
else
  echo"Log directory not found please check file path:" ${LOG_FILE}
  exit 1
echo ""
fi
exit $status




