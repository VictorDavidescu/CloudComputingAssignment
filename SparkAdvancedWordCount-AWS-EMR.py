################################################################################
#   TITLE   : Spark Advanced Word Count for Amazon Web Services(AWS) - 
#             Elastic Map Reduce (EMR)
#   AUTHOR  : Victor Florian Davidescu
#   REV DATE: 23/03/2021
################################################################################
# Standard packages
import os
import math
import sys
import re
import argparse

# Third-party packages
from pyspark.sql.functions import col
from pyspark.sql import SparkSession


################################################################################
#   NAME        : CleanLine()
#   DESCRIPTION : Removes all non letter characters from each line
################################################################################
def CleanLine(line):

    # Check if line is not empty or contains only new line symbol
    if line != "" and line != "\n":

        # Remove new line character
        line = line.replace('\n','')

        # Lower all cases in the line
        line = line.lower()

        line = re.sub("[^a-z]"," ",line)
    
        # Replace multiple whitespaces with only one whitespace
        line = " ".join(line.split())

    # Return the cleaned line
    return line


################################################################################
#   NAME        : ObtainWords()
#   DESCRIPTION : Returns a list of words from each line
################################################################################
def ObtainWords(line):

    wordArray = []

    # Iterate through each word from the line
    for word in line.split():

        # Check if word has more than 1 character length
        if(len(word) > 1):
            wordArray.append(word)

    # Return the word array
    return wordArray


################################################################################
#   NAME        : ObtainLetters()
#   DESCRIPTION : Returns a list of words from each line
################################################################################
def ObtainLetters(word):

    lettersArray = []

    # Iterate through each letter
    for letter in word.split():

        # Check if letter is not dash
        if(letter != '-'):
            lettersArray.extend(letter)

    # Return the letters array
    return lettersArray


################################################################################
#   NAME        : GetThresholds()
#   DESCRIPTION : Returns thresholds for the total distinct words/letters
################################################################################
def ObtainThresholds(totalDistinctDataNo):

    # Set the percentages
    percentageHigh = 0.05
    percentageMiddle = 0.05
    percentageLow = 0.05
    maxPercentage  = 1.0
    dataWastePercentage = maxPercentage - (percentageHigh + percentageMiddle + percentageLow)

    # Calculate the thresholds
    rankPopularThreshold        = int(math.ceil(totalDistinctDataNo * percentageHigh))
    rankCommonLeftThreshold    = int(math.floor(totalDistinctDataNo * (percentageHigh + dataWastePercentage / 2) ))
    rankCommonRightThreshold    = int(math.ceil(totalDistinctDataNo * (maxPercentage - percentageLow - dataWastePercentage / 2) ))
    rankRareThreshold           = int(math.floor(totalDistinctDataNo * (maxPercentage - percentageLow) ))

    # Return the thresholds
    return (rankPopularThreshold, rankCommonLeftThreshold, rankCommonRightThreshold, rankRareThreshold)


################################################################################
#   NAME        : Main()
#   DESCRIPTION : This method will perform the following steps:
#                 
#                 1) Obtain lines from the sample file
#                 2) Clean lines from non-letter characters
#                 3) Obtain all words from lines
#                 4) Obtain all letters from lines
#                 5) Obtain the distinct words, place their frequency with their index
#                 6) Obtain the distinct letters, place their frequency with their index
#                 7) Obtain threshold values for popular/common/rare word groups
#                 8) Obtain threshold values for popular/common/rare letter groups
#                 9) Create dataframe for all distinct words
#                 10) Create dataframes for popular/common/rare word groups
#                 11) Create dataframe for all distinct letters
#                 12) Create dataframes for popular/common/rare letter groups
#                 13) Save the stdout to a txt file then move it to an S3 bucket
#                   
################################################################################
def Main(bucketName, inputSamplesDir, outputSamplesDir, sampleFileName):

    # Construct the full path to the sample file
    sampleFullPath = os.path.join(bucketName, inputSamplesDir, sampleFileName)

    # Construct the full path to the output sample file
    outputSampleFileName = "output-{0}".format(sampleFileName)
    outputSampleFullPath = os.path.join(bucketName, outputSamplesDir, outputSampleFileName)
    
    print("The print staments will be outputed on file: {0}".format(outputSampleFileName))
    f = open(outputSampleFileName,'w')
    sys.stdout = f

    # Create a spark session
    with SparkSession.builder.getOrCreate() as spark:
        
        # Read file and add to an RDD object
        rddLines = spark.sparkContext.textFile(sampleFullPath)

        # Clean each line from the file
        rddCleanedLines = rddLines.map(lambda line: CleanLine(line))

        # Obtain words from each line
        rddWords = rddCleanedLines.flatMap(lambda line: ObtainWords(line))

        # Obtain letters from each line and remove empty spaces and dashes
        rddLetters = rddCleanedLines.flatMap(lambda line: [char for char in line])
        rddLetters = rddLetters.filter(lambda char: char!=' ' and char!='-')

        # Obtain frequencies for each word, re-arrange the key and sort it
        rddWordsFrequency = rddWords.map(lambda word: (word,1)).reduceByKey(lambda word, freq: word + freq)
        rddWordsFrequency = rddWordsFrequency.map(lambda x:(x[1], x[0]))
        rddWordsFrequency = rddWordsFrequency.sortByKey(False)
        rddWordsFrequency = rddWordsFrequency.zipWithIndex()
        rddWordsFrequency = rddWordsFrequency.map(lambda x: (x[1]+1, x[0][1], x[0][0]))

        # Obtain frequencies for each letter, re-arrange the key and sort it
        rddLettersFrequency = rddLetters.map(lambda letter: (letter,1)).reduceByKey(lambda letter, freq: letter + freq)
        rddLettersFrequency = rddLettersFrequency.map(lambda x:(x[1], x[0]))
        rddLettersFrequency = rddLettersFrequency.sortByKey(False)
        rddLettersFrequency = rddLettersFrequency.zipWithIndex()
        rddLettersFrequency = rddLettersFrequency.map(lambda x: (x[1]+1, x[0][1], x[0][0]))
        
        # Obtain total number of words and letters
        totalWordsNo = len(rddWords.collect())
        totalDistinctWordsNo = len(rddWordsFrequency.collect())
        totalDistinctLettersNo = len(rddLettersFrequency.collect())

        # Obtain threshold values for words
        (rankPopularWordThreshold,
            rankCommonLeftWordThreshold, 
            rankCommonRightWordThreshold, 
            rankRareWordThreshold) = ObtainThresholds(totalDistinctWordsNo)

        # Obtain threshold values for letters
        (rankPopularLetterThreshold,
            rankCommonLeftLetterThreshold, 
            rankCommonRightLetterThreshold, 
            rankRareLetterThreshold) = ObtainThresholds(totalDistinctLettersNo)

        print("---------------------------------------------------------------------------------------------")
        print("Author: Victor-Florian Davidescu")
        print("SID: 1705734")
        print("Output for file: {0}".format(sampleFileName))
        print("---------------------------------------------------------------------------------------------")
        print("Total number of words: {0}".format(totalWordsNo))
        print("Total number of distinct words: {0}".format(totalDistinctWordsNo))
        print("Popular words threshold: {0}".format(rankPopularWordThreshold))
        print("Common words left threshold: {0}".format(rankCommonLeftWordThreshold))
        print("Common words right threshold: {0}".format(rankCommonRightWordThreshold))
        print("Rare words threshold: {0}".format(rankRareWordThreshold))
        print("---------------------------------------------------------------------------------------------")

        # Create overall word dataframe
        dataFrameWords = spark.createDataFrame(rddWordsFrequency,schema=["Rank", "Word", "Frequency"])

        # Create popular/common/rare words dataframe
        dataFramePopularWords = dataFrameWords.where(col("Rank").between(1, rankPopularWordThreshold))
        dataFrameCommonWords = dataFrameWords.where(col("Rank").between(rankCommonLeftWordThreshold, rankCommonRightWordThreshold))
        dataFrameRareWords = dataFrameWords.where(col("Rank").between(rankRareWordThreshold, totalDistinctWordsNo))

        print("")
        print("Popular Words")
        dataFramePopularWords.show(dataFramePopularWords.count())
        print("Common Words")
        dataFrameCommonWords.show(dataFrameCommonWords.count())
        print("Rare Words")
        dataFrameRareWords.show(dataFrameRareWords.count())

        print("---------------------------------------------------------------------------------------------")
        print("Total number of distinct letters: {0}".format(totalDistinctLettersNo))
        print("Popular letters threshold: {0}".format(rankPopularLetterThreshold))
        print("Common letters left threshold: {0}".format(rankCommonLeftLetterThreshold))
        print("Common letters right threshold: {0}".format(rankCommonRightLetterThreshold))
        print("Rare letters threshold: {0}".format(rankRareLetterThreshold))
        print("---------------------------------------------------------------------------------------------")

        # Create letter dataframe
        dataFrameLetters = spark.createDataFrame(rddLettersFrequency,schema=["Rank","Letter","Frequency"])

        # Create popular/common/rare words dataframe
        dataFramePopularLetters = dataFrameLetters.where(col("Rank").between(1, rankPopularLetterThreshold))
        dataFrameCommonLetters = dataFrameLetters.where(col("Rank").between(rankCommonLeftLetterThreshold, rankCommonRightLetterThreshold))
        dataFrameRareLetters = dataFrameLetters.where(col("Rank").between(rankRareLetterThreshold, totalDistinctLettersNo))

        print("")
        print("Popular Letters")
        dataFramePopularLetters.show(dataFramePopularLetters.count())
        print("Common Letters")
        dataFrameCommonLetters.show(dataFrameCommonLetters.count())
        print("Rare Letters")
        dataFrameRareLetters.show(dataFrameRareLetters.count())
        print("---------------------------------------------------------------------------------------------")

        f.close()
        sys.stdout = sys.__stdout__

        print("Program finished successfully, check the output file {0}".format(outputSampleFileName))

    # Send the output file to s3 bucket
    os.system("aws s3 mv {0} {1}".format(outputSampleFileName, outputSampleFullPath))


################################################################################
#   STARTING POINT | Arguments
################################################################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('bucketName')
    parser.add_argument('inputSamplesDir')
    parser.add_argument('outputSamplesDir')
    parser.add_argument('sampleFileName')
    args = parser.parse_args()

    Main(args.bucketName, args.inputSamplesDir, args.outputSamplesDir, args.sampleFileName)
