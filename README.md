# CS4225_HadoopAndSpark
This repository details 2 assignments completed in NUS CS4225 module. The 2 assignments requires implementation of solution using Hadoop and Spark respectively.

<ins>Assignment 1: Hadoop</ins>

Given 2 textual files, for each common word between the files, find the smaller number of times the word appears between the files. In general, words with different case or different non-whitespace punctuation are considered different words. Additionally, common words (case sensitive) cannot appear in the stopwords.txt file provided. Output the top 20 common words with the highest frequency. 

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
