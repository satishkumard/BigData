
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._

object CensusDataAnalysis {

  def main(args: Array[String]) {

    if (args.length < 4) {
      println("Usage: CensusDataAnalysis <inputFolder> <outputFile1> <outputFile2> <outputFile3")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Census Data Analysis")
    val sc = new SparkContext(sparkConf)


    /* age fertility rates */
    val age_fert = sc.textFile(args(0)+"/age_specific_fertility_rates.csv")
    val age_fert_hdr = age_fert.first()
    val age_fert_rdd = age_fert.filter(row => row!=age_fert_hdr).map(row => row.split(","))


    /* mortality life expectancy */
    val mortality_life = sc.textFile(args(0)+"/mortality_life_expectancy.csv")
    val mortality_life_hdr = mortality_life.first()
    val mortality_life_rdd = mortality_life.filter(row => row!=mortality_life_hdr).map(row => row.split(","))


    /* birth death growth rates */
    val birth_death = sc.textFile(args(0)+"/birth_death_growth_rates.csv")
    val birth_death_hdr = birth_death.first()
    val birth_death_rdd = birth_death.filter(row => row!=birth_death_hdr).map(row => row.split(","))


    /* mid year population */
    val midyear_pop = sc.textFile(args(0)+"/midyear_population_age_sex.csv")
    val midyear_pop_hdr = midyear_pop.first()
    val midyear_pop_rdd = midyear_pop.filter(row => row!=midyear_pop_hdr).map(row => row.split(","))

    /* Life expectancy by gender overall - as of 2018 */

    val life_expectancy_male_overall= mortality_life_rdd.filter(row => row(2)<="2018")
      .map(array => array(7).toDouble)
      .reduce(_+_)/mortality_life_rdd.filter(row => row(2)<="2018").count()

    val life_expectancy_female_overall=mortality_life_rdd.filter(row => row(2)<="2018")
      .map(array => array(8).toDouble)
      .reduce(_+_)/mortality_life_rdd.filter(row => row(2)<="2018").count()

    val life_expectancy_male_overall_by_year=mortality_life_rdd.filter(row => row(2)<="2018")
      .map(array => (array(2),array(7).toDouble)).mapValues(value => (value,1))
      .reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR) }
      .mapValues { case(sum, count) => sum / count }.sortByKey(ascending=true)

    val life_expectancy_female_overall_by_year=mortality_life_rdd.filter(row => row(2)<="2018")
      .map(array => (array(2),array(8).toDouble)).mapValues(value => (value,1))
      .reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR) }
      .mapValues { case(sum, count) => sum / count }.sortByKey(ascending=true)



    /* Life expectancy by country, visualize high and low life expectancy - as of 2018 */

    val life_expectancy_overall_by_country=mortality_life_rdd.filter(array => array(2)<="2018")
      .map(array => (array(1),array(6).toDouble)).mapValues(value => (value,1))
      .reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR) }
      .mapValues { case(sum, count) => sum / count }.sortByKey(ascending=true)


    /* Birth rate Vs. Death rate by country */

    val birth_rate_by_country=birth_death_rdd.filter(array => array(2)<="2018")
      .map(array => (array(1),array(3).toDouble)).mapValues(value => (value,1))
      .reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR) }
      .mapValues { case(sum, count) => sum / count }.sortByKey(ascending=true)

    val death_rate_by_country=birth_death_rdd.filter(array => array(2)<="2018")
      .map(array => (array(1),array(4).toDouble)).mapValues(value => (value,1))
      .reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR) }
      .mapValues { case(sum, count) => sum / count }.sortByKey(ascending=true)

    val birth_death_rate_by_country = birth_rate_by_country.join(death_rate_by_country).sortByKey(ascending=true)



    /* Male/Female ratio at birth */

    val male_female_ratio_at_birth_by_country = age_fert_rdd.filter(array=>array(2)<="2018")
      .map(array => (array(1),array(12).toDouble)).mapValues(value => (value,1))
      .reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR) }
      .mapValues { case(sum, count) => sum / count }.sortByKey(ascending=true)


    /* Migration rate by country */

    val migration_rate_by_country = birth_death_rdd.filter(array => array(2)<="2018")
      .map(array => (array(1),array(5).toDouble)).mapValues(value => (value,1))
      .reduceByKey { case ((sumL, countL), (sumR, countR)) => (sumL + sumR, countL + countR) }
      .mapValues { case(sum, count) => sum / count }.sortByKey(ascending=true)


    /* Population balance (United States) */

    val indices = Array.range(5,106)

    val male_us_pop_by_year = midyear_pop_rdd
      .filter(array => array(1)=="United States" && array(3)=="Male" && array(2)<="2018")
      .map(array => (array(2).toInt,indices.map(array).map(_.toInt).sum))

    val female_us_pop_by_year = midyear_pop_rdd
      .filter(array => array(1)=="United States" && array(3)=="Female" && array(2)<="2018")
      .map(array => (array(2).toInt,indices.map(array).map(_.toInt).sum))

    /* Function to merge to CSV files */
    def merge(srcPath: String, dstPath: String): Unit =  {
      val hadoopConfig = new Configuration()
      val hdfs = FileSystem.get(hadoopConfig)
      FileUtil.copyMerge(hdfs, new Path(srcPath), hdfs, new Path(dstPath), true, hadoopConfig, null)
    }


    /* File1: Census Data Analysis */
    life_expectancy_overall_by_country
    .map{ case(x,y) => (x,(x,y))}
    .join(birth_death_rate_by_country).join(male_female_ratio_at_birth_by_country).join(migration_rate_by_country)
      .values.map{case(x,y)=> x._1._1._1 + "," + x._1._1._2 + "," + x._1._2._1  + "," + x._1._2._2  + "," + x._2 + "," + y}
      .repartition(1).saveAsTextFile(args(1))

    /* File2: Population Balance */
    male_us_pop_by_year.map{ case(x,y) => (x,(x,y))}.join(female_us_pop_by_year).sortByKey()
      .values.map{case(x,y)=> "US" + "," + x._1 + "," + x._2 + "," + y}.repartition(1).saveAsTextFile(args(2))

    /* File3: Life Expectancy */
    life_expectancy_female_overall_by_year.map{ case(x,y)=> (x,(x,y)) }.join(life_expectancy_male_overall_by_year)
      .values.sortBy(_._1).map{ case(x,y) => x._1 + "," + x._2 + "," + y + "," + life_expectancy_female_overall + "," + life_expectancy_male_overall }
      /*.union("year" + "," + "life_expec_female" + "," + "life_expec_male" + "," + "life_expectancy_female_overall" + "," + "life_expectancy_male_overall") */
      .repartition(1).saveAsTextFile(args(3))

    merge(args(1), args(1)+".csv")
    merge(args(2), args(2)+".csv")
    merge(args(3), args(3)+".csv")

    System.exit(0)
  }
}
