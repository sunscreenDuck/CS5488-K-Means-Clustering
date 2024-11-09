## sbt project compiled with Scala 3

### Usage

This is a normal sbt project. You can compile code with `sbt compile`, run it with `sbt run`, and `sbt console` will start a Scala 3 REPL.

For more information on the sbt-dotty plugin, see the
[scala3-example-project](https://github.com/scala/scala3-example-project/blob/main/README.md).

### pre-requisite:
#### Hadoop installation
- if you are using windows system
- make sure you have installed hadoop properly.
- you may follow the guide: https://kontext.tech/article/377/latest-hadoop-321-installation-on-windows-10-step-by-step-guide
- for installation and configuration of hadoop in windows environment

#### Run project
- you are suggested to run the project with intelliJ community version with scala plugin.
- Open the project in administrative mode

#### Output
- The project will output the k means clustering plot data as csv files: standard_player.csv
- Then we need to import the data by another python project and use plot lib to generate the cluster picture  