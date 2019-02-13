# Twitter Sentiment Analysis Engine
I developed a Long Short-Term Memory Recurrent Neural Network to predict Twitter sentiment. The data was streamed directly into a Postgres database leveraging the Twitter API. Using advanced NLP techniques such as Word2Vec and TF-IDF vectorization, I compared different approaches to data cleaning and the impacts on predictive modeling. The final Neural Network was built using PyTorch. All coding was performed on Google Colaboratory running on a Google cloud compute GPU.

## Project background
Please read the project_proposal.pdf detailing the goals and methodology behind the project.
## Data Files
1.	trump.csv contains the original data freshly exported from PostgreSQL 
2.	clean_labeled_data.csv contains the datafile with added features, such as categorical labels, parsed word tokens, etc.
## Notebooks
1.	data_cleaning_eda.ipynb shows all the data cleaning and exploratory data analysis. 
2.	models.ipynb shows only the modeling. Models include Word2Vec family of models, classification models, and the LSTM RNN classification model.
3.	twitter_sentiment_analysis_engine.ipynb show’s the entire project contents. 
4.	twitter_streamer.ipynb shows the code for obtaining the tweets and storing them into PostgreSQL.


