# MERGER

This project merges two files that were written in the binary form. The process first convert the original files into .dat files (It reads 
all files into memory and then convert them). Based on new .dat files, the new reader will read all .dat files up to a sequence number retrieved
from the information of million seconds corresponding to each record. Then the processor will deal with all the data in the chunk. This procedure
is repeated until we touch the end of one of the files.

## Function/Class Description

Some classes are copied from the classes given by Prof. Lee Maclin such as all the interface and DBManger. The new classes created are listed
below:

### Functions

* The DATGeneration.java convert the original binary file into .dat files. To use this class, the user should input the base input file
directory and output file directory. Then the user should supplement the subdirectory of each file. The user should proceed to run the 
generation of output directory and stock ID methods. Once the user have done the preparation work, the launch method can be called.
* The DATReader.java reads the files that we have converted. Each time it receives the filepath and read it.
* The DATProcessor.java is used to merge the file. It receives the output directory and linked list of DATReader objects and merges them 
based on the sequence number.
* The Merger.java puts all the functionality together. Basically it receives all the directories of input files as a linked list and a output file
directory. The launch method will merge the input files and output the new contents into the output directory.
* The Test_MergerAndJoin.java is modified to test all merger process. Details are given in the test procedure.

## Implementation of the Merger.

The implementation of the whole project can be tested in the Test_MergerAndJoin.java file. To do the test, the user should create a junit
test and import all the necessary libraries. The file paths should be modified according to your system. For some reason, the directories
other than the default project path cannot be accepted. This phenomenon happened when I tested it. If this appears, I guess that it has
something to do with your system (In particular Windows 10 Pro) due to the accessbility authority. Thus, I suggested that the user put 
the files under the default directory to avoid unnecessary debug since the FileNOTFound error appeared to be mysterious. 

## Authors

* **Xiao Guan** - *Initial work* - [MERGER](https://github.com/guan4015/MERGER)


## Acknowledgments

The author thanks Professor Lee Maclin for his help on this assignment.
