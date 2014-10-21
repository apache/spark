from py4j.java_gateway import JavaGateway

class ClassNone(object):

    def getName(self):
        return None

    class Java:
        implements = ['py4j.examples.InterfaceNone']

if __name__ == '__main__':
    gateway = JavaGateway(start_callback_server=True)
    objectNone = ClassNone()
    returnValue = gateway.entry_point.testNone(objectNone)
    print(returnValue)
    gateway.shutdown()
