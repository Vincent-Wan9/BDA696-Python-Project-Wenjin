from pyspark import keyword_only
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCols, HasOutputCol
from pyspark.ml.util import DefaultParamsReadable, DefaultParamsWritable
from pyspark.sql import functions as func
from pyspark.sql.window import Window


class BattingAverageTransform(
    Transformer,
    HasInputCols,
    HasOutputCol,
    DefaultParamsReadable,
    DefaultParamsWritable,
):
    @keyword_only
    def __init__(self, inputCols=None, outputCol=None):
        super(BattingAverageTransform, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)
        return

    @keyword_only
    def setParams(self, inputCols=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        input_cols = self.getInputCols()
        output_col = self.getOutputCol()

        window_spec = (
            Window.partitionBy(input_cols[0])
            .orderBy(input_cols[1])
            .rangeBetween(-100, 0)
        )

        dataset = dataset.withColumn(
            output_col,
            func.sum(input_cols[2]).over(window_spec)
            / func.sum(input_cols[3]).over(window_spec),
        )

        return dataset
