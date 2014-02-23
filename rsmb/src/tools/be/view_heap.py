"""
/*******************************************************************************
 * Copyright (c) 2007, 2013 IBM Corp.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Eclipse Distribution License v1.0 which accompany this distribution. 
 *
 * The Eclipse Public License is available at 
 *    http://www.eclipse.org/legal/epl-v10.html
 * and the Eclipse Distribution License is available at 
 *   http://www.eclipse.org/org/documents/edl-v10.php.
 *
 * Contributors:
 *    Ian Craggs - initial API and implementation and/or initial documentation
 *******************************************************************************/

        Extract heap dump from a file containing some other data, e.g. an FFDC

"""


import sys, os

start_eyecatcher = "=========== Start of heap dump ==========\n"
end_eyecatcher = "\n=========== End of heap dump =========="

infile = open(sys.argv[1], "rb")
contents = infile.read()
infile.close()

symbols = ""
if contents.find("MQTTS") != -1:
	symbols += "-D MQTTS "
if contents.find("epoll") != -1:
	symbols += "-D USE_POLL "

outfile = open("heapdump", "wb")
outfile.write(contents[contents.find(start_eyecatcher) + len(start_eyecatcher):-len(end_eyecatcher)])
outfile.close()

if os.name == "posix":
	call = "LD_LIBRARY_PATH=$LD_LIBRARY_PATH:. be -i rsmb.ini "+symbols+" rsmb\!heapdump"
else:
	call = "be -i rsmb.ini "+symbols+" rsmb!heapdump"

os.system(call)


