#include <iostream>
#include <vector>
#include <utility>
#include <fstream>
#include <mpi.h>
#include <string>
#include <sstream>
#include <algorithm>
#include <limits>


using namespace std;

struct mypair
{
	float key;
	char* value;
}
;

bool comparator(mypair a, mypair b)
{
	// check equal to case
	return a.key<b.key;
}

int main(int argc, char* argv[])
{

	FILE * fp;
	long fil_sz;
	float * buffer;
	char *val_buffer;
	size_t result;
	int num_proc;
	int pid;


	MPI_Init(&argc, &argv);
	MPI_Comm_size (MPI_COMM_WORLD, &num_proc);
	int total_col=atoi(argv[1]);
	int priv_col=total_col/num_proc;
	float infinity=std::numeric_limits<float>::infinity();

	int add_cols=0;
	if(total_col%num_proc!=0)
		add_cols=num_proc-(total_col%num_proc);
	
	priv_col=(total_col+add_cols)/num_proc;

	MPI_Comm_rank (MPI_COMM_WORLD, &pid);
	// vector my_intvec;
	vector <int> maxkeynum;
	vector <int> maxvalsize;
	vector<vector<mypair> > columns;

	// read the files and store in columns
	for(int i=0;i<priv_col;i++)
	{
		string str= argv[2];

		stringstream strs;
		int number=pid*priv_col+i+1;
		if(number>total_col)
			break;

	  	strs << number;
	  	string temp_str = strs.str();
	  	str.append(temp_str);
	  	char* f_num = (char*) str.c_str();
		fp = fopen ( f_num , "rb" );
		if (fp==NULL) 
		{			
			fputs ("File error",stderr); 
			// cout<<"qwertyuiopqwertyuiop"<<endl;
			exit (1);
		}

		buffer = (float*) malloc (4);

		if (buffer == NULL) 
		{
			fputs ("Memory error",stderr); 
			// cout<<"xcvbnmzxcvbnm"<<endl;
			exit (2);
		}
		vector<mypair> thiscolumn;

		fseek (fp , 0 , SEEK_END);
		fil_sz = ftell (fp);
		rewind (fp);
		// copy the file into the buffer:
		// if (result != fil_sz/4) {fputs ("Reading error",stderr); exit (3);}
		int * int_buffer;
		int_buffer = (int*) malloc (4);
		result = fread (int_buffer,4,1,fp);

		int n = int_buffer[0];
		maxvalsize.push_back(n);
		// cout << n << endl;
		val_buffer = (char*) malloc (n);
		int num_keys = (fil_sz-4)/(n+4);

		maxkeynum.push_back(num_keys);
		for(int i=0;i<num_keys;i++)
		{
			mypair temp;
			fread (buffer,4,1,fp);
			// cout << buffer[0]<< endl;
			temp.key=buffer[0];
			fread (val_buffer,1,n,fp);
			temp.value=val_buffer;
			// cout << val_buffer << endl;
			thiscolumn.push_back(temp);
			// cout<<temp.key<<" "<<temp.value<<endl;
		}

		columns.push_back(thiscolumn);
		fclose (fp);
		free (buffer);

	}

	total_col+=add_cols;
		// exit(1);


	// for testing purposes------------------------------------------------
	
	// --------------------------------------------------------------------

	int max_keysize= *std::max_element(maxkeynum.begin(), maxkeynum.end());
	int val_size= *std::max_element(maxvalsize.begin(), maxvalsize.end());
	int * temp_maxkeysz=(int*) malloc((num_proc)*sizeof(int));
	int* temp_maxvalsz=(int*) malloc(num_proc*sizeof(int));
	MPI_Allgather(&max_keysize,1,MPI_INT, temp_maxkeysz, 1, MPI_INT, MPI_COMM_WORLD);
	MPI_Allgather(&val_size,1,MPI_INT, temp_maxvalsz, 1, MPI_INT, MPI_COMM_WORLD);
	// cout<<"qwertyuioasdfghjzxcvbnm"<<endl;
	int overall_maxkeysz=*std::max_element(temp_maxkeysz, temp_maxkeysz+num_proc);
	int overall_maxvalsz=*std::max_element(temp_maxvalsz, temp_maxvalsz+num_proc);	
	int rem=overall_maxkeysz % num_proc;
	if(rem!=0)
		overall_maxkeysz+=(num_proc-rem);
		// exit(1);

	for(int i=0;i<columns.size();i++)
	{
		for(int j=0;j<columns[i].size();j++)
		{
			string temp=columns[i][j].value;
			char * appender= (char*) malloc(overall_maxvalsz*sizeof(char));
			appender=&temp[0u];
			// appender+=temp.length();
			for(int kk=temp.length();kk<(overall_maxvalsz);kk++)
			{
				appender[kk]='\0';
			}

			columns[i][j].value=appender;
		}
		// cout<<columns[i].size()<<" "<<pid<<endl;

		while(columns[i].size()<overall_maxkeysz)
		{
			mypair temp;
			temp.key=infinity;
			char * appender= (char*) malloc(overall_maxvalsz*sizeof(char));
			for(int kk=0;kk<overall_maxvalsz;kk++)
			{
				appender[kk]='\0';
				// cout<<"dhoom dhadaka"<<endl;
			}

			temp.value=appender;
			columns[i].push_back(temp);
		}


	}

		// exit(1);
	// cout<<"qwerty"<<endl;
		// exit(1);

	while(columns.size()!=priv_col)
	{

		vector<mypair> my_vec;
		while(my_vec.size()<overall_maxkeysz)
		{
			mypair temp;
			temp.key=infinity;
			char * appender= (char*) malloc(overall_maxvalsz*sizeof(char));
			for(int kk=0;kk<overall_maxvalsz;kk++)
			{
				appender[kk]='\0';
				// cout<<"dhoom dhadaka"<<endl;
			}

			temp.value=appender;
			my_vec.push_back(temp);
		}
		columns.push_back(my_vec);

	}



	// cout<<"sdfghjhgfxcvhjijvb"<<endl;

// printing
	// exit(1);

	// cout<<pid<<endl;
	// for(int i=0;i<columns[0].size();i++)
	// {
	// 	for(int j=0;j<columns.size();j++)
	// 	{
	// 		// cout<<"pid= "<<pid<<columns[j][i].key<<" ";
	// 		for(int k=0;k<overall_maxvalsz;k++)
	// 		{
	// 			// if(columns[j][i].value[k]=='\0')
	// 				// break;
	// 			cout<<columns[j][i].value[k];
	// 			// result = fwrite (&rows2[i][j].value[k],1,1,fp);
	// 		}
	// 		cout<<" ";
	// 	}
	// 	cout<<endl;
	// }
	// cout<<"hello"<<endl;
	// cout<<endl<<endl<<endl<<endl<<endl;

		// exit(1);
	vector <vector<mypair> > rows2;
	float* linear_key=(float*) malloc(priv_col*overall_maxkeysz*sizeof(float));
	char* linear_val=(char*) malloc(priv_col*overall_maxvalsz*overall_maxkeysz*sizeof(char));

	float* recd_linear_key=(float*) malloc(priv_col*overall_maxkeysz*sizeof(float));
	char* recd_linear_val=(char*) malloc(priv_col*overall_maxvalsz*overall_maxkeysz*sizeof(char));

	float* linear_keyrow=(float*) malloc(priv_col*overall_maxkeysz*sizeof(float));
	char* linear_valrow=(char*) malloc(priv_col*overall_maxvalsz*overall_maxkeysz*sizeof(char));

	float* recd_linear_keyrow=(float*) malloc(priv_col*overall_maxkeysz*sizeof(float));
	char* recd_linear_valrow=(char*) malloc(priv_col*overall_maxvalsz*overall_maxkeysz*sizeof(char));
	
	float* linear_key2=(float*) malloc(priv_col*overall_maxkeysz*sizeof(float));
	char* linear_val2=(char*) malloc(priv_col*overall_maxvalsz*overall_maxkeysz*sizeof(char));
	
	float* recd_linear_key2=(float*) malloc(priv_col*overall_maxkeysz*sizeof(float));
	char* recd_linear_val2=(char*) malloc(priv_col*overall_maxvalsz*overall_maxkeysz*sizeof(char));





	while(1)
	{
		
		int index=0;
		int char_ind=0;

		// linearise the columns

		for(int j=0;j<columns[0].size();j++)
		{
			for(int i=0;i<columns.size();i++)
			{
				linear_key[index]=columns[i][j].key;
				index++;
				for(int k=0;k<overall_maxvalsz;k++)
				{
					linear_val[char_ind]=columns[i][j].value[k];
					char_ind++;
				}
			}
		}

		// ----------------------------------------------------------------

		

		MPI_Alltoall(linear_key, priv_col*overall_maxkeysz/num_proc , MPI_FLOAT, recd_linear_key, priv_col*overall_maxkeysz/num_proc, MPI_FLOAT, MPI_COMM_WORLD);
		MPI_Alltoall(linear_val, priv_col*overall_maxkeysz*overall_maxvalsz/num_proc , MPI_CHAR, recd_linear_val , priv_col*overall_maxkeysz * overall_maxvalsz/num_proc , MPI_CHAR, MPI_COMM_WORLD);

		// cout<<recd_linear_key[1]<<endl;
		// received the columns from outside

		// printing--------------------------------------------------------------

		vector <vector<mypair> > rows;

		index=0;
		char_ind=0;
		// cout<<"hhhhhh"<<endl;
		// free(linear_key);
		// free(linear_val);

		// store the received columns in the respective rows

		for(int j=0;j<columns[0].size()/num_proc;j++)
		{
			vector<mypair> my_vec;
			for(int nproc=0;nproc<num_proc;nproc++)
			{
				for(int i=0;i<columns.size();i++)
				{
					// linear_key[index]=columns[i][j].key;
					mypair temp;
					temp.key=recd_linear_key[nproc*priv_col*overall_maxkeysz/num_proc + i + j*priv_col];
					char* temp_val= (char*) malloc(overall_maxvalsz*sizeof(char));
					for(int k=0;k<overall_maxvalsz;k++)
					{
						// iska bharosa nahi hai
						temp_val[k]=recd_linear_val[nproc*priv_col*overall_maxkeysz*overall_maxvalsz/num_proc+ k + i*overall_maxvalsz + j*priv_col*overall_maxvalsz];
						// char_ind++;
					}
					temp.value=temp_val;
					my_vec.push_back(temp);

				}
			}

			rows.push_back(my_vec);
		}

		// free(recd_linear_val);
		// free(recd_linear_key);
		// -------------------------------------------------------------

		for(int i=0;i<rows.size();i++)
		{
			sort(rows[i].begin(), rows[i].end(), comparator);
		}

		// cout<<endl<<endl<<endl<<endl<<endl<<endl;

		// for(int i=0;i<rows.size();i++)
		// {
		// 	for(int j=0;j<rows[i].size();j++)
		// 	{
		// 		cout<<rows[i][j].key;
			
		// 		for(int k=0;k<overall_maxvalsz;k++)
		// 		{
		// 			// if(columns[j][i].value[k]=='\0')
		// 				// break;
		// 			cout<<rows[i][j].value[k];
		// 			// result = fwrite (&rows2[i][j].value[k],1,1,fp);
		// 		}
		// 		cout<<" ";
		// 	}
		// 	cout<<endl;
		// }

		// cout<<endl<<endl<<endl<<endl<<endl<<endl;

		

		index=0;
		char_ind=0;

		// linearise the data in rows------------

		for(int j=0;j<rows[0].size();j++)
		{
			for(int i=0;i<rows.size();i++)
			{
				linear_keyrow[index]=rows[i][j].key;
				index++;
				for(int k=0;k<overall_maxvalsz;k++)
				{
					linear_valrow[char_ind]=rows[i][j].value[k];
					char_ind++;
				}
			}
		}

		// --------------------------------------



		MPI_Alltoall(linear_keyrow,priv_col*overall_maxkeysz/num_proc, MPI_FLOAT,recd_linear_keyrow, priv_col*overall_maxkeysz/num_proc, MPI_FLOAT, MPI_COMM_WORLD);
		MPI_Alltoall(linear_valrow,priv_col*overall_maxkeysz*overall_maxvalsz/num_proc, MPI_CHAR,recd_linear_valrow, priv_col*overall_maxkeysz*overall_maxvalsz/num_proc, MPI_CHAR, MPI_COMM_WORLD);

		columns.clear();
		rows2.clear();

		// put the received linearised data back in the columns

		for(int j=0;j<rows[0].size()/num_proc;j++)
		{
			vector<mypair> my_vec;
			for(int nproc=0;nproc<num_proc;nproc++)
			{
				for(int i=0;i<rows.size();i++)
				{
					// linear_key[index]=columns[i][j].key;
					mypair temp;
					temp.key=recd_linear_keyrow[nproc*priv_col*overall_maxkeysz/num_proc+i + j*rows.size()];
					// nproc*priv_col*overall_maxkeysz/num_proc + i + j*priv_col
					char* temp_val= (char*) malloc(overall_maxvalsz*sizeof(char));
					for(int k=0;k<overall_maxvalsz;k++)
					{
						// iska bharosa nahi hai
						temp_val[k]=recd_linear_valrow[nproc*priv_col*overall_maxkeysz*overall_maxvalsz/num_proc + k + i*overall_maxvalsz + j*rows.size()*overall_maxvalsz];
					}
					temp.value=temp_val;
					my_vec.push_back(temp);

				}
			}

			columns.push_back(my_vec);
		}

		for(int i=0;i<columns.size();i++)
		{
			sort(columns[i].begin(), columns[i].end(), comparator);
		}


		// row ka hagga----------------------------------------


		rows.clear();
		index=0;
		char_ind=0;

		// linearise the columns

		for(int j=0;j<columns[0].size();j++)
		{
			for(int i=0;i<columns.size();i++)
			{
				linear_key2[index]=columns[i][j].key;
				index++;
				for(int k=0;k<overall_maxvalsz;k++)
				{
					linear_val2[char_ind]=columns[i][j].value[k];
					char_ind++;
				}
			}
		}

		// ----------------------------------------------------------------



		MPI_Alltoall(linear_key2, priv_col*overall_maxkeysz/num_proc , MPI_FLOAT,recd_linear_key2, priv_col*overall_maxkeysz/num_proc, MPI_FLOAT, MPI_COMM_WORLD);
		MPI_Alltoall(linear_val2, priv_col*overall_maxkeysz*overall_maxvalsz/num_proc , MPI_CHAR, recd_linear_val2 , priv_col*overall_maxkeysz * overall_maxvalsz/num_proc , MPI_CHAR, MPI_COMM_WORLD);

		// cout<<recd_linear_key[1]<<endl;
		// received the columns from outside

		// printing--------------------------------------------------------------

		index=0;
		char_ind=0;
		// cout<<"hhhhhh"<<endl;

		// store the received columns in the respective rows

		for(int j=0;j<columns[0].size()/num_proc;j++)
		{
			vector<mypair> my_vec;
			for(int nproc=0;nproc<num_proc;nproc++)
			{
				for(int i=0;i<columns.size();i++)
				{
					// linear_key[index]=columns[i][j].key;
					mypair temp;
					temp.key=recd_linear_key2[nproc*priv_col*overall_maxkeysz/num_proc + i + j*priv_col];
					char* temp_val= (char*) malloc(overall_maxvalsz*sizeof(char));
					for(int k=0;k<overall_maxvalsz;k++)
					{
						// iska bharosa nahi hai
						temp_val[k]=recd_linear_val2[nproc*priv_col*overall_maxkeysz*overall_maxvalsz/num_proc+ k + i*overall_maxvalsz + j*priv_col*overall_maxvalsz];
						// char_ind++;
					}
					temp.value=temp_val;
					my_vec.push_back(temp);

				}
			}

			rows2.push_back(my_vec);
		}


		int flag=0;
		for(int i=0;i<rows2.size()-1;i++)
		{
			for(int j=0;j<rows2[i].size()-1;j++)
			{
				if(rows2[i][j].key>rows2[i][j+1].key)
				{
					flag=1;
					break;
				}
				if(rows2[i][j].key>rows2[i+1][j].key)
				{
					flag=1;
					break;
				}
			}

			if(flag==1)
				break;
		}
		if(flag==0)
		break;



	}


	// exit(1);

	// cout<<"ertyujbkkkkkkkkkkk"<<endl;


	string str= argv[2];

	stringstream strs;
	// int number=pid*priv_col+i+1;
  	strs << 0;
  	string temp_str = strs.str();
  	str.append(temp_str);
  	char* f_num = (char*) str.c_str();

	
	int file_free=0;

	if ( pid == 0 ) 
	{
		file_free = 1;
		fp = fopen ( f_num , "w" );
		if (fp==NULL) 
		{
			fputs ("File error",stderr); 
			exit (1);
		}
	}
	else 
	{
		MPI_Recv(&file_free, 1, MPI_INT, pid-1, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
		fp = fopen ( f_num , "a" );
		if (fp==NULL) 
		{
			fputs ("File error",stderr); 
			exit (1);
		}

	}
	if (file_free == 1)
	{

		for(int i=0;i<rows2.size();i++)
		{
			for(int j=0;j<rows2[i].size();j++)
			{
				if(rows2[i][j].key==infinity)
					continue;
				fwrite (&rows2[i][j].key,4,1,fp);
				// cout<< rows2[i][j].key<<" ";
				for(int k=0;k<overall_maxvalsz;k++)
				{
					if(rows2[i][j].value[k]=='\0')
						break;
					// cout<<rows2[i][j].value[k];
					fwrite (&rows2[i][j].value[k],1,1,fp);
				}
				// cout<<" ";

			}
			// myfile<<endl;
			// cout<<endl;
		}

		fclose(fp);
	}
	// give read file permission to the next process
	if (pid != num_proc-1) 
		MPI_Send(&file_free, 1, MPI_INT, pid+1, 1, MPI_COMM_WORLD);
	
	// if(pid==num_proc-1)
	// 	fclose(fp);







	/* the whole file is now loaded in the memory buffer. */

	// terminate

	MPI_Finalize();
	return 0;

}














