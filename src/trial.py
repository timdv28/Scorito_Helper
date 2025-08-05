from procyclingstats import Rider
import pprint

rider = Rider("rider/tadej-pogacar")
rider.birthdate()
rider.parse()
pprint.pprint(rider.parse())
