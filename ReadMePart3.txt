Part3 solution is implemented in the file named 'Part3.ipynb' using Jupyter notebook.

To run this file 'Part3.ipynb':
	1. Open the whole project in Pycharm
	2. Open a 'Terminal'  window in Pycharm by click "Terminal" button below.
	3. Run command 'pip install jupyter' in the Terminal to install jupyter library. If you have already installed Jupyter notebook
in your enviroment, you can skip this step.
	4. Run command 'jupyter notebook' in the Terminal. It will open a Jupyter notebook webpage in you web browser aotomatically.
	5. You are now in the Jupyter notebook webpage. Click file ''Part3.ipynb" to open it.
	6. You are now in the file  ''Part3.ipynb". The file now shows all the outputs of each cell when I ran it.  You can run every cell you want 
by click the cell first then hit the 'run' button of Jupyter notebook. 

	Important note : The cell 'In[16]' ( docummented with '# Determine optimal number of clusters by using Silhoutte Score Analysis.' ) and 
'In[17]' ( docummented with '#Visualizing the silhouette scores in a plot' ) use Silhoutte Score Analysis to determine optimal number of clusters in K-means algorithym. 
I have tested them using 'ml-latest-small' dataset. It will take several hours to run these two cells. So, for just seeing the final solutions of part 3, you would 
better to SKIP these two cells to save running time.

	6.1 To cluster users by movie taste: You run each cell until cell 'In[24]' (SKIP or the two cells I mentioned) . The program will now generate a
csv file named "prediction.csv" in your Jupyter notebook home webpage. This file is the prediction result of all users' cluster numbers. Also, the program will output the 
predicted cluster number of user 57 in the cell "out[24]" by default. 

	If you want to get predicted cluster number of other specific user, update the num '57' with the new user ID in cell 'In[24]'.
                Example: get_prediction(64)   //64 is new id

	NOTE: The existing file 'prediction.csv' is the result saved when I ran it.

	6.2 To provide movie recommendations for the user you input just now: You run the remaining cells, which is, till cell "In[28]". 
The program will output two tables for  recommendations  of user 57 in the cell "out[28]" by default .

	If you want to get  movie recommendations of other specific user, update the num '57' with the new user ID in cell 'In[28]'.
                Example: movie_recommendation(64)  //64 is new id

	