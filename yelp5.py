# in each city average rating given to each category

from mrjob.job import MRJob
from mrjob.protocol import JSONValueProtocol

def average_rat(iterable):
        count = 0
        sum = 0.0

        for item in iterable:
                sum += item
                count += 1

        return sum / count


class MRCityCatRating(MRJob):
        INPUT_PROTOCOL = JSONValueProtocol

        def mapper(self, _, business):
                
                city = business['city']
                rating = business['stars']
                buss = business['business_id']
                categories = business['categories']
              
                for category in categories:
                        yield( (city , category,buss), rating)

                 
        def reducer(self, city_cat, rating):
                avg = average_rat(rating)
                
                yield ( city_cat, avg)
    
    
if __name__ == '__main__':
        MRCityCatRating.run()

