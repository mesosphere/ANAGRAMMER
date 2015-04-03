def dot(url_list, definer_map, output_file):
  f = open(output_file, 'w')

  print definer_map
  for word, definition in definer_map.iteritems():
      f.write("%s: %s \n" % (word, definition))

  f.close()

  print "Writing results to " + output_file
