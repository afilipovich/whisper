#!/usr/bin/env python

import os
import sys
import signal
import optparse
import mmap

try:
  import whisper
except ImportError:
  raise SystemExit('[ERROR] Please make sure whisper is installed properly')

# Ignore SIGPIPE
signal.signal(signal.SIGPIPE, signal.SIG_DFL)

option_parser = optparse.OptionParser(
    usage='''%prog [options] from_path to_path''')
option_parser.add_option(
    '--cc', default=False,
    action='store_true',
    help="Carbon Copy merge. Source and target WSP archives retention schemas must be exactly the same.")

(options, args) = option_parser.parse_args()

if len(args) < 2:
  option_parser.print_usage()
  sys.exit(1)

path_from = args[0]
path_to = args[1]

for filename in (path_from, path_to):
   if not os.path.exists(filename):
       raise SystemExit('[ERROR] File "%s" does not exist!' % filename)

def mmap_file(filename):
  fd = os.open(filename, os.O_RDONLY)
  map = mmap.mmap(fd, 0, prot=mmap.PROT_READ)
  os.close(fd)
  return map

def seriesStart(archive, map):
    step = archive['secondsPerPoint']
    base_offset = archive['offset']
    for point in xrange(archive['points']):
      offset = base_offset + point * whisper.pointSize
      (timestamp, value) = whisper.struct.unpack(whisper.pointFormat, map[offset:offset+whisper.pointSize])
      if timestamp != 0:
        return timestamp - point * step
    return None

def ccmerge(path_from, path_to):
  """Carbon-copy style of merge: instead of propagating datapoints
  from the source WSP file through archives in the destination WSP file
  the datapoint is copied to the corresponding slot in corresponding archives.
  Works only with archives having the same retention schema.
  Only datapoints missing in the destination archive are copied
  (i.e. existing datapoints in the destination archive do not get overwritten)"""
  import fcntl
  map_from = mmap_file(path_from)
  fd = os.open(path_to, os.O_RDWR)
  fcntl.flock(fd, fcntl.LOCK_EX)
  map_to = mmap.mmap(fd, 0, prot=mmap.PROT_WRITE)
  dstHeader = whisper.info(path_from)
  srcHeader = whisper.info(path_to)
  srcArchives = srcHeader['archives']
  dstArchives = dstHeader['archives']
  for srcArchive, dstArchive in zip(srcArchives, dstArchives):
    for p in ('points', 'secondsPerPoint'):
      if srcArchive[p] != dstArchive[p]:
        raise Exception, "%s and %s have different number of %s: %d vs %d" % (path_from, path_to, p, srcArchive[p], dstArchive[p])

  for srcArchive, dstArchive in zip(srcArchives, dstArchives):
    srcArchiveStart = seriesStart(srcArchive, map_from)
    dstArchiveStart = seriesStart(dstArchive, map_to)
    # source archive is empty => nothing to copy
    if srcArchiveStart is None:
        continue
    # destination archive is empty => does not matter which postition we put data to
    if dstArchiveStart is None:
        dstArchiveStart = 0
    # find the difference in alignment
    step = srcArchive['secondsPerPoint']
    alignmentDiff = (dstArchiveStart - srcArchiveStart) / step # offset in number of datapoints between source and destination archives
    # iterate through points and copy them
    base_offset = srcArchive['offset']
    points = srcArchive['points']
    for pointInSrc in xrange(points):
      pointInDst = pointInSrc - alignmentDiff
      # archive is circular
      if pointInDst < 0:
        pointInDst += points
      if pointInDst >= points:
        pointInDst -= points
      dstOffset = base_offset + pointInDst * whisper.pointSize
      srcOffset = base_offset + pointInSrc * whisper.pointSize
      (dstTimestamp, dstValue) = whisper.struct.unpack(whisper.pointFormat, map_to[dstOffset:dstOffset+whisper.pointSize])
      # we have the datapoint in the destination arhive, no need to copy
      if dstTimestamp != 0:
        continue
      (srcTimestamp, srcValue) = whisper.struct.unpack(whisper.pointFormat, map_from[srcOffset:srcOffset+whisper.pointSize])
      # datapoint is missing in the source archive as well, nothing to copy
      if srcTimestamp == 0:
        continue
      # copy the datapoint
      map_to[dstOffset:dstOffset+whisper.pointSize] = whisper.struct.pack(whisper.pointFormat, srcTimestamp, srcValue)
  map_to.flush()
  os.close(fd)

if options.cc:
    ccmerge(path_from, path_to)
else:
    whisper.merge(path_from, path_to)

