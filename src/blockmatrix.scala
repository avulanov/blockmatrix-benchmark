import org.apache.spark.mllib.linalg.Matrices
import org.apache.spark.mllib.linalg.distributed.BlockMatrix
val parallelism = 5
val rows = 100000
val columns = 10000
val size = rows * columns
val blockSize = 10000
assert(rows % blockSize == 0)
assert(columns % blockSize == 0)
val rowBlocks = rows / blockSize
val columnBlocks = columns / blockSize
val rdd = sc.parallelize( { 
	for(i <- 0 until rowBlocks; j <- 0 until columnBlocks) yield (i, j) 
	}, parallelism).map( coord => (coord, Matrices.rand(blockSize, blockSize, util.Random.self)))
val bm = new BlockMatrix(rdd, blockSize, blockSize).cache()
bm.validate()
val t = System.nanoTime()
val ata = bm.transpose.multiply(bm)
ata.validate()
println(rows + "x" + columns + ", block:" + blockSize + "\t" + (System.nanoTime() - t) / 1e9) 