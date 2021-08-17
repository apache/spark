# Apache Spark

Spark என்பது பெரிய அளவிலான தரவு செயலாக்கத்திற்கான ஒருங்கிணைந்த பகுப்பாய்வு இயந்திரமாகும். இது வழங்குகிறது
ஸ்காலா, ஜாவா, பைத்தான் மற்றும் ஆர் ஆகியவற்றில் உயர் மட்ட ஏபிஐகள் மற்றும் ஒரு உகந்த இயந்திரம்
தரவு பகுப்பாய்வு பொது கணக்கீட்டு வரைபடங்கள் ஆதரிக்கிறது. இது ஒரு ஆதரிக்கிறது
உயர் மட்ட கருவிகள் பணக்கார தொகுப்பு உட்பட ஸ்பார்க் எஸ்க்யூஎல் மற்றும் டேட்டாபிரேம்கள்,
பாண்டஸ் APஐ மீது பாண்டாக்கள் பணிச்சுமைகள், இயந்திர கற்றல் Mmலிப், வரைபட செயலாக்க கிராப்எக்ஸ்,
மற்றும் ஸ்ட்ரீம் செயலாக்க கட்டமைக்கப்பட்ட ஸ்ட்ரீமிங்.

<https://spark.apache.org/>

[![GitHub Action Build](https://github.com/apache/spark/actions/workflows/build_and_test.yml/badge.svg?branch=master&event=push)](https://github.com/apache/spark/actions/workflows/build_and_test.yml?query=branch%3Amaster+event%3Apush)
[![Jenkins Build](https://amplab.cs.berkeley.edu/jenkins/job/spark-master-test-sbt-hadoop-3.2/badge/icon)](https://amplab.cs.berkeley.edu/jenkins/job/spark-master-test-sbt-hadoop-3.2)
[![AppVeyor Build](https://img.shields.io/appveyor/ci/ApacheSoftwareFoundation/spark/master.svg?style=plastic&logo=appveyor)](https://ci.appveyor.com/project/ApacheSoftwareFoundation/spark)
[![PySpark Coverage](https://codecov.io/gh/apache/spark/branch/master/graph/badge.svg)](https://codecov.io/gh/apache/spark)


## Online Documentation

நிரலாக்கம் உட்பட சமீபத்திய ஸ்பார்க் ஆவணத்தை நீங்கள் காணலாம்
வழிகாட்டி, மீது [project web page](https://spark.apache.org/documentation.html).
இந்த README கோப்பு அடிப்படை அமைப்பு வழிமுறைகளை மட்டுமே கொண்டுள்ளது.

## Building Spark

Spark பயன்படுத்தி கட்டப்பட்டது [Apache Maven](https://maven.apache.org/).
ஸ்பார்க் மற்றும் அதன் எடுத்துக்காட்டு நிரல்களை உருவாக்க, இயக்கவும்:

    ./build/mvn -DskipTests clean package

(You do not need to do this if you downloaded a pre-built package.)

More detailed documentation is available from the project site, at
["Building Spark"](https://spark.apache.org/docs/latest/building-spark.html).

For general development tips, including info on developing Spark using an IDE, see ["Useful Developer Tools"](https://spark.apache.org/developer-tools.html).

## Interactive Scala Shell

ஸ்பார்க் பயன்படுத்த தொடங்க எளிதான வழி ஸ்காலா ஷெல் மூலம்:

    ./bin/spark-shell

பின்வரும் கட்டளையை முயற்சிக்கவும், இது 1,000,000,000 திரும்ப வேண்டும்:

    scala> spark.range(1000 * 1000 * 1000).count()

## Interactive Python Shell

மாற்றாக, நீங்கள் பைத்தான் விரும்பினால், நீங்கள் பைத்தான் ஷெல் பயன்படுத்தலாம்:

    ./bin/pyspark

பின்வரும் கட்டளையை இயக்கவும், இது 1,000,000,000 திரும்ப வேண்டும்:

    >>> spark.range(1000 * 1000 * 1000).count()

## Example Programs

ஸ்பார்க் 'எடுத்துக்காட்டுகள்' கோப்பகத்தில் பல மாதிரி நிரல்களுடன் வருகிறது.
அவற்றில் ஒன்றை இயக்க, பயன்படுத்தவும் `./bin/run-example <class> [params]`. For example:

    ./bin/run-example SparkPi

will run the Pi example locally.

நீங்கள்  MASTER-il அமைக்க முடியும் environment variable when running examples to submit
examples to a cluster. This can be a mesos:// or spark:// URL,
"yarn" to run on YARN, and "local" to run
locally with one thread, or "local[N]" N நூல்களுடன் உள்ளூரில் இயங்க வேண்டும் . நீங்கள்
வகுப்பு இருந்தால் சுருக்கமான வகுப்புப் பெயரையும் பயன்படுத்தலாம்  `examples`
package. For instance:

    MASTER=spark://host:7077 ./bin/run-example SparkPi

பராம் எதுவும் கொடுக்கப்படாவிட்டால் பல உதாரணத் திட்டங்கள் பயன்பாட்டு அச்சிட உதவும்.

## Running Tests

முதலில் சோதனை தேவை [building Spark](#building-spark). ஸ்பார்க் கட்டப்பட்டவுடன், சோதனைகள்
இதைப் பயன்படுத்தி இயக்கலாம்:
    ./dev/run-tests

Please see the guidance on how to
[run tests for a module, or individual tests](https://spark.apache.org/developer-tools.html#individual-tests).

குபெர்னடீஸ் ஒருங்கிணைப்பு சோதனையும் உள்ளது, see resource-managers/kubernetes/integration-tests/README.md

## A Note About Hadoop Versions

எச்டிஎஃப்எஸ் மற்றும் பிற ஹடூப் ஆதரவுடன் பேச ஹடூப் கோர் லைப்ரரியை ஸ்பார்க் பயன்படுத்துகிறது
சேமிப்பு அமைப்புகள். ஏனெனில் நெறிமுறைகள் வெவ்வேறு பதிப்புகளில் மாறிவிட்டன
ஹடூப், உங்கள் கிளஸ்டர் இயங்கும் அதே பதிப்பிற்கு எதிராக நீங்கள் தீப்பொறியை உருவாக்க வேண்டும்.

தயவுசெய்து உருவாக்க ஆவணத்தை பார்க்கவும்
["Specifying the Hadoop Version and Enabling YARN"](https://spark.apache.org/docs/latest/building-spark.html#specifying-the-hadoop-version-and-enabling-yarn)
ஹடூப்பின் ஒரு குறிப்பிட்ட விநியோகத்திற்கான கட்டிடம் பற்றிய விரிவான வழிகாட்டுதலுக்காக
குறிப்பிட்ட ஹைவ் மற்றும் ஹைவ் த்ரிஃப்ட் சர்வர் விநியோகங்களுக்கான கட்டிடம்.

## Configuration

தயவுசெய்து பார்க்கவும் [Configuration Guide](https://spark.apache.org/docs/latest/configuration.html)
ஸ்பார்க்கை எவ்வாறு கட்டமைப்பது என்பது பற்றிய ஒரு கண்ணோட்டத்திற்கான ஆன்லைன் ஆவணத்தில்.

## Contributing

தயவுசெய்து பார்க்கவும் [Contribution to Spark guide](https://spark.apache.org/contributing.html)
திட்டத்திற்கு எவ்வாறு பங்களிக்கத் தொடங்குவது என்பது பற்றிய தகவலுக்கு.
