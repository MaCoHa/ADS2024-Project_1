from collections import defaultdict
import re


def remove_special_characters_and_numbers(text):
    # This will keep only alphabetic characters and spaces
        return re.sub(r'[^A-Za-z\s]', '', text)

class traningdata:
    def __init__(self):
        self.word = ""
        self.PROB_NEGATIVE = float(0)
        self.PROB_POSITIVE = float(0)
        self.processedlist = list()
        
        
        
        
    def process(self, val):
        if val["label"] == 0 or val["label"] == 4:           
            self.processedlist.append((val["label"], val["text"].lower()))
      
  


    def end_partition(self):
      # sort words into bag
        dict = defaultdict(lambda: defaultdict(int))
        word_set = set()
        negative_count = 0
        positive_count = 0
        total_count = 0
        
        
        for label,text in self.processedlist:
           clean_string = remove_special_characters_and_numbers(text)
           clean_array = clean_string.split(" ")
           for j in clean_array:
                if j == "":
                  continue
              
                word_set.add(j)
                  
                total_count += 1
                
                if label == 0:
                    negative_count += 1
                elif (label == 4):
                    positive_count += 1
                    
                dict[j][label] = dict[j][label] + 1
                
       
        
        min_number = 1e-322
        
        for word in word_set:
            negative_prob = max(min_number,((dict[word][0] + 1) / (negative_count + len(word_set))))
            positive_prob = max(min_number,((dict[word][4] + 1) / (positive_count + len(word_set)))) 
            yield(word,negative_prob,positive_prob,negative_count,positive_count,total_count)   
            
      
        
test_data = [
    {
    "label": 4, 
    "text": "very powerful"
  },
    {
    "label": 4, 
    "text": "the most fun film of the summer"
  },
    {
    "label": 0, 
    "text": "just plain boring"
  },
    {
    "label": 0, 
    "text": "entirely predictable and lacks energy"
  },
    {
    "label": 0, 
    "text": "no surprises and very few laughs"
  }
    ]


t = traningdata()

# Process the test data
for i in test_data:
    t.process(i)

# Run end_partition and print the results
for result in t.end_partition():
    print(result)

