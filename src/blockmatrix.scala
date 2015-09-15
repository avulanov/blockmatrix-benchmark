// Block-diagonal matrix test
// R = M * M
// number of multiplications == parallelism
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
val maxParallelism = 5
val blockSize = 10000
for (parallelism <- 1 to maxParallelism) {
  val rowBlocks = parallelism
  val columnBlocks = rowBlocks
  val rows = blockSize * rowBlocks
  val columns = blockSize * columnBlocks
  val rdd = sc.parallelize( { 
    for (i <- 0 until rowBlocks) yield (i, i) 
    }, parallelism).map( coord => (coord, Matrices.rand(blockSize, blockSize, util.Random.self)))
  val bm = new BlockMatrix(rdd, blockSize, blockSize).cache()
  bm.validate()
  val t = System.nanoTime()
  val ata = bm.multiply(bm)
  ata.validate()
  println(rows + "x" + columns + ", block:" + blockSize + "\t" + (System.nanoTime() - t) / 1e9) 
}

// Block-columnar matrix test
// R = M * M^T
// number of multiplications == parallelism^2
import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
val maxParallelism = 5
val blockSize = 10000
for (parallelism <- 1 to maxParallelism) {
  val rowBlocks = parallelism
  val columnBlocks = 1
  val rows = blockSize * rowBlocks
  val columns = blockSize * columnBlocks
  val rdd = sc.parallelize( { 
    for(i <- 0 until rowBlocks) yield (i, 0) 
    }, parallelism).map( coord => (coord, Matrices.rand(blockSize, blockSize, util.Random.self)))
  val bm = new BlockMatrix(rdd, blockSize, blockSize).cache()
  bm.validate()
  val mb = bm.transpose.cache()
  mb.validate()
  val t = System.nanoTime()
  val ata = bm.multiply(mb)
  ata.validate()
  println(rows + "x" + columns + ", block:" + blockSize + "\t" + (System.nanoTime() - t) / 1e9) 
}
