from pyspark import SparkContext
from pyspark.java_gateway import ensure_callback_server_started

class PythonListener(object):
    package = "com.nvidia.spark.rapids.listener"

    @staticmethod
    def get_manager():
        jvm = SparkContext.getOrCreate()._jvm
        manager = getattr(jvm, "{}.{}".format(PythonListener.package, "Manager"))
        return manager

    def __init__(self):
        self.uuid = None
        self.failures = []

    def notify(self, obj):
        """This method is required by Scala Listener interface
        we defined above.
        """
        self.failures.append(obj)

    def register(self):
        ensure_callback_server_started(gw = SparkContext.getOrCreate()._gateway)
        manager = PythonListener.get_manager()
        self.uuid = manager.register(self)
        return self.uuid

    def unregister(self):
        manager =  PythonListener.get_manager()
        manager.unregister(self.uuid)
        self.uuid = None

    class Java:
        implements = ["com.nvidia.spark.rapids.listener.Listener"]
