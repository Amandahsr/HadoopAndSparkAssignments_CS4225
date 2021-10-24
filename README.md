# CS4225_HadoopAndSpark
This repository details proposed solution to 2 assignments completed in NUS CS4225 module. The 2 assignments require implementation of code using Hadoop and Spark respectively.

<ins>**Assignment 1: Hadoop**</ins>

Given 2 textual files (Task1-input1.txt, Task1-input2.txt), for each common word between the files, find the smaller number of times the word appears between the files. In general, words with different case or different non-whitespace punctuation are considered different words. Additionally, common words (case sensitive) cannot appear in the stopwords.txt file provided. Output the top 20 common words with the highest frequency. 

Proposed Solution:
1. Mapper
   - Input both textual files.
   - Split each input file into individual words using delimiters " \t\n\r\f".
   - Obtain filename that each word comes from.
   - Remove words that appear in the stopwords file.
   - Output (word, filename) key-value pairs.
   
2. Reducer
   - Input (word, filename) key-value pairs.
   - For each word, count number of times each filename occurs.
   - Retain the the lower number of times each word appears.
   - Add (lower frequency, word) key-value pairs into a linked hashmap and sort by values in descending order.
   - Output the top 20 (lower frequency, word) key-value pairs.

<ins>**Assignment 2: Spark**</ins>

A team of engineers has planted a bomb somewhere in NUS. You have to find the password and the IP address of their engineering server to login and disarm it. Using the 2 log files with the help of the 2 NOTES files, query for the required information.

Proposed Solution:
1. Finding password to server
   - Find error message that came from the Gamma subsystem by removing all messages containing 'alpha' and 'beta'.
   - Find error message that shows "Invalid user" to obtain password.
   
2. Finding IP address of server
   - 'dst' server IP appears only on the weekly session (0,7,14,21,28) days. 
   - There are only 13 personal IPs recorded for the dst server IP, for each of the 13 bomb engineers.
