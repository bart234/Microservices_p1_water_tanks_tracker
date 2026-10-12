class FakeKafkaClassStructure:
    '''expected value and header in form like usual came from kafka
    values: binary dict
    headers: list of tuples with sond value binary'''
    def __init__(self,fake_raw_values,fake_raw_header):
        self.fake_raw_values = fake_raw_values
        self.fake_raw_header = fake_raw_header

    def headers(self):
        return self.fake_raw_header

    def value(self):
        return self.fake_raw_values 
    