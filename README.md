# InterviewTest

1. Download: git clone https://github.com/maxteneff/InterviewTest.git
2. Build: cd InterviewTest && sbt
3. Run: spark-submit --master local[2] ./interviewtest_2.11-1.0.jar localhost 9999 file:/tmp output.txt
4. Look for output file: tail -f output.txt