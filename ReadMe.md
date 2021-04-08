# Project structure :
1. _cli_app_ folder contains the source code relative to the interactive command line interface implemented with PyInquirer library.
2. _web_app_ folder contains the source code of the web application impleemnted with Flask and responsible for hosting the Bokeh application to display results on the browser
3. _data_ folder contains the datasets from MovieLens website
4. _core_ contains the implementation of the requirements using Spark 

# Running the program :

You will need two terminals:
1. The first to run the web application by executing the following command from the root folder of the project :
`python -m web_app.app`
   
2. The second to run the CLI application by executing the following command from the root folder of the project :
`python -m cli_app.app`

# Part 2 - Design details : 

Find the favourite genre of a given user, or group of users：
	This is done by calculate the formula : count * factor + average_rating * ( 1 - factor) on users' ratings and movies watched.
	count is the number of specific genre movies watched.
 	average_rating is average rating of  user group to this genre's movie.
	factor is the weight value of count. That's to say, how much important do you think count is when deciding your favorite genre.

Compare the movie tastes of two users:
     	Movie taste is compared in two aspects. 
	1. Number of common watched movies by two users. Also the whole information of the movies and ratings of them will be showed in table.
                2. similarity score out of 10 based on two users' ratings on these common movies.
                First calculate point of each rating pairs and then sum them up. 
                Finally the sum would be averaged to make the similarity score.

	Calculation formular of point is as following:
	Same Rating (eg 5 and 5): 10pts,

	½ Rating difference (eg 4 and 4½): 8pts

	1 Rating difference (eg 5 and 4): 6pts

	1 ½ Rating difference (eg 4 and 2 ½): 5pts

	2 Rating difference (eg 4 and 2): 4pts

	2 ½ Rating difference (eg 4 and 1 ½): 3pts

	3 Rating difference (eg 4 and 1): 2pts

	3 ½ Rating difference (eg 5 and 1 ½): 1pt

	4 Rating difference (eg 5 and 1): 0.5pts

	4 ½ Rating difference (eg 5 and ½): 0 pts
                
	You can see that gap is not evenly distributed. that is because some thoughts on the significance of scoring gap.
	if User A were to rate a movie with 4.5 stars (meaning they thought it was near masterpiece level), and User B were to rate it with 3.5 stars
	 (meaning they thought it was good but still flawed), even though it’s only 1 star, 
	it’s still a noticeable gap in tastes that went from almost perfect to an passable film.
	Therefore, the similarity score wouldn’t really merit an 8 (which would mean someone you have many similarities with), 
	it would be more of a 6 (which means someone who kind of likes what you like but not to the same degree.
         	So, that’s why  it's best to have a larger gap between ratings, as the smaller the gap, the less the change in rating would mean.

# Part 3 :

Part3 solution is implemented in the file named 'Part3.ipynb' using Jupyter notebook.

To run this file 'Part3.ipynb':
1. Open the whole project in Pycharm
2. Open a 'Terminal'  window in Pycharm by click "Terminal" button below.
3. Run command 'pip install jupyter' in the Terminal to install jupyter library. If you have already installed Jupyter notebook
in your enviroment, you can skip this step.
4. Run command 'jupyter notebook' in the Terminal. It will open a Jupyter notebook webpage in you web browser aotomatically.
5. Before running the file "Part3.ipynb". You need to fist upload your own datasets "movies.csv" and "ratings.csv" from your computer.
6. You are now in the Jupyter notebook webpage. Click file "Part3.ipynb" to open it.
7. You are now in the file  ''Part3.ipynb". The file now shows all the outputs of each cell when I ran it.  You can run every cell you want
by click the cell first then hit the 'run' button of Jupyter notebook.

	Important note : The cell 'In[16]' ( docummented with '# Determine optimal number of clusters by using Silhoutte Score Analysis.' ) and 
'In[17]' ( docummented with '#Visualizing the silhouette scores in a plot' ) use Silhoutte Score Analysis to determine optimal number of clusters in K-means algorithym.
I have tested them using 'ml-latest-small' dataset. It will take several hours to run these two cells. So, for just seeing the final solutions of part 3, you would
better to SKIP these two cells to save running time.

	7.1 To cluster users by movie taste: You run each cell until cell 'In[24]' (SKIP or the two cells I mentioned) . The program will now generate a
csv file named "prediction.csv" in your Jupyter notebook home webpage. This file is the prediction result of all users' cluster numbers. Also, the program will output the
predicted cluster number of user 57 in the cell "out[24]" by default.

	If you want to get predicted cluster number of other specific user, update the num '57' with the new user ID in cell 'In[24]'.
                Example: get_prediction(64)   //64 is new id

	NOTE: The existing file 'prediction.csv' is the saved result when I ran it.

	7.2 To provide movie recommendations for the user you input just now: You run the remaining cells, which is, till cell "In[28]". 
The program will output two tables for  recommendations  of user 57 in the cell "out[28]" by default .

	If you want to get  movie recommendations of other specific user, update the num '57' with the new user ID in cell 'In[28]'.
                Example: movie_recommendation(64)  //64 is new id
   
# References :
1. Bokeh Development Team (2018). Bokeh: Python library for interactive visualization
   URL http://www.bokeh.pydata.org.
   
2. Jesse Vig, Shilad Sen, and John Riedl. 2012. The Tag Genome: Encoding Community Knowledge to Support Novel Interaction. ACM Trans. Interact. Intell. Syst. 2, 3: 13:1–13:44. <https://doi.org/10.1145/2362394.2362395>

3. https://github.com/CITGuru/PyInquirer/