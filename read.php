<?php
$fh = fopen("feeds/www_hispacasas_com_20170412112633440048.xml", "r");

$line = 0;

while (($buffer = fgets($fh)) !== FALSE) {
   if ($line == 319850) {
       print $buffer;

   }   
   if ($line == 319851) {
       print $buffer;
   }   
   if ($line == 319852) {
       print $buffer;
   }   
   if ($line == 319853) {
       print $buffer;
       break;
   }   
   $line++;
}