'''
Created on Dec 19, 2009

@author: barthelemy
'''
from py4j.java_gateway import JavaGateway

if __name__ == '__main__':
    gateway = JavaGateway()
    buffer = gateway.getStringBuffer()
    buffer.append(True)
    buffer.append(1.0)
    buffer.append('This is a Python {0}'.format('string'))
    print(buffer.toString())
