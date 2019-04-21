use strict;
use warnings;

use PostgresNode;
use TestLib;
use Test::More;

my $node = get_new_node('main');
$node->init();
$node->start();

# invoke psql
# - opts: space-separated options and arguments
# - stat: expected exit status
# - in: input stream
# - out: list of re to check on stdout
# - err: list of re to check on stderr
# - name: of the test
sub psql
{
	local $Test::Builder::Level = $Test::Builder::Level + 1;
	my ($opts, $stat, $in, $out, $err, $name) = @_;
	my @cmd = ('psql', split /\s+/, $opts);
	$node->command_checks_all(\@cmd, $stat, $out, $err, $name, $in);
	return;
}

psql('-c \\q', 0, '', [ qr{^$} ], [ qr{^$} ], 'psql -c');
psql('', 0,"\\echo hello\n\\warn world\n\\q\n", [ qr{^hello$} ], [ qr{^world$} ], 'psql in/out/err');

$node->stop();
done_testing();
