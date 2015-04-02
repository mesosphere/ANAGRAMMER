def dot(url_list, render_map, output_file):
  f = open(output_file, 'w')

  print render_map
  for word, definition in render_map.iteritems():
      f.write("%s: %s \n" % (word, definition))

  f.close()

  print "Writing results to " + output_file
