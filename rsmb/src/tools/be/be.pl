#!/usr/bin/perl

#/*******************************************************************************
# * Copyright (c) 2008, 2013 IBM Corp.
# *
# * All rights reserved. This program and the accompanying materials
# * are made available under the terms of the Eclipse Public License v1.0
# * and Eclipse Distribution License v1.0 which accompany this distribution. 
# *
# * The Eclipse Public License is available at 
# *    http://www.eclipse.org/legal/epl-v10.html
# * and the Eclipse Distribution License is available at 
# *   http://www.eclipse.org/org/documents/edl-v10.php.
# *
# * Contributors:
# *    Nicholas O'Leary - initial API and implementation and/or initial documentation
# *******************************************************************************/

use File::Spec;

# usage: be.pl Broker.h ../rsmb.ini

my $root = ".";
my $output = "rsmb.ini";
my $srcDir;

ARG:while($_ = shift )
{ 
   study;
   /^-h$/ and usage();
   /-s(.*)/ and do { $root = $1 || shift ; next ARG;};
   /-o(.*)/ and do { $output = $1|| shift  ; next ARG;};
   /[^-]|-[^so]/ and usage();
}

sub usage {
   print "
Usage: be.pl -s srcdir -o output
   -s srcdir : this is the directory containing Broker.h. default: '.'
   -o output : the file to write the be definition to. default: 'rsmb.ini'

";
   exit 0;
}


my %processedFiles = ();

sub processFile {
   my $out = $_[0]; 
   my $file = $_[1];
   if (!exists($processedFiles{$file})) {
      open(IN,"<$file") or die "Failed to process '$file': $!";
      my @lines = <IN>;
      close(IN);
      print $out "// ************** $file **************\n";
      my $inblock = 0;
      foreach $line  (@lines) {
         $line =~ s/\r\n/\n/g;
         if ($inblock) {
            if ($line =~ /^(.*)BE\*\//) {
               print $out $1;
               $inblock = 0;
            } else {
               if ($line =~ /^include "(.*)"/) {
                  processFile($out, File::Spec->catfile(($root),"$1.h"));
               } else {
                  print $out $line;
               }
            }
         } else {
            if ($line =~ /^\/\*BE(.*)/) {
               print $out $1;
               $inblock = 1;
            }
         }
      }
      $processedFiles{$file} = 1;
   }
}

open(OUT,"> $output") or die "$output: $!";
processFile(*OUT, File::Spec->catfile(($root),'Broker.h'));
close(OUT);
