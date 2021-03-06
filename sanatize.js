'use strict';

module.exports = sanatize;
var reserved = new Set([
  'all',
  'analyse',
  'analyze',
  'and',
  'any',
  'array',
  'as',
  'asc',
  'asymmetric',
  'authorization',
  'between',
  'binary',
  'both',
  'case',
  'cast',
  'check',
  'collate',
  'column',
  'constraint',
  'create',
  'cross',
  'current_date',
  'current_role',
  'current_time',
  'current_timestamp',
  'current_user',
  'default',
  'deferrable',
  'desc',
  'distinct',
  'do',
  'else',
  'end',
  'except',
  'false',
  'for',
  'foreign',
  'freeze',
  'from',
  'full',
  'grant',
  'group',
  'having',
  'ilike',
  'in',
  'initially',
  'inner',
  'intersect',
  'into',
  'is',
  'isnull',
  'join',
  'leading',
  'left',
  'like',
  'limit',
  'localtime',
  'localtimestamp',
  'natural',
  'new',
  'not',
  'notnull',
  'null',
  'off',
  'offset',
  'old',
  'on',
  'only',
  'or',
  'order',
  'outer',
  'overlaps',
  'placing',
  'primary',
  'references',
  'right',
  'select',
  'session_user',
  'similar',
  'some',
  'symmetric',
  'table',
  'then',
  'to',
  'trailing',
  'true',
  'union',
  'unique',
  'user',
  'using',
  'verbose',
  'when',
  'where',
  'xmin',
  'xmax',
  'format',
  'controller',
  'action',
  'oid',
  'tableoid',
  'xmin',
  'cmin',
  'xmax',
  'cmax',
  'ctid',
  'ogc_fid'
]);
var subs = [
    [/<[^>]+>/gm, ''],
    [/[àáâãäåāă]/g, 'a'],
    [/æ/g, 'ae'],
    [/[ďđ]/g, 'd'],
    [/[çćčĉċ]/g, 'c'],
    [/[èéêëēęěĕė]/g, 'e'],
    [/ƒ/g, 'f'],
    [/[ĝğġģ]/g, 'g'],
    [/[ĥħ]/g, 'h'],
    [/[ììíîïīĩĭ]/g, 'i'],
    [/[įıĳĵ]/g, 'j'],
    [/[ķĸ]/g, 'k'],
    [/[łľĺļŀ]/g, 'l'],
    [/[ñńňņŉŋ]/g, 'n'],
    [/[òóôõöøōőŏŏ]/g, 'o'],
    [/œ/g, 'oe'],
    [/ą/g, 'q'],
    [/[ŕřŗ]/g, 'r'],
    [/[śšşŝș]/g, 's'],
    [/[ťţŧț]/g, 't'],
    [/[ùúûüūůűŭũų]/g, 'u'],
    [/ŵ/g, 'w'],
    [/[ýÿŷ]/g, 'y'],
    [/[žżź]/g, 'z'],
    [/[ÀÁÂÃÄÅĀĂ]/gi, 'A'],
    [/Æ/gi, 'AE'],
    [/[ĎĐ]/gi, 'D'],
    [/[ÇĆČĈĊ]/gi, 'C'],
    [/[ÈÉÊËĒĘĚĔĖ]/gi, 'E'],
    [/Ƒ/gi, 'F'],
    [/[ĜĞĠĢ]/gi, 'G'],
    [/[ĤĦ]/gi, 'H'],
    [/[ÌÌÍÎÏĪĨĬ]/gi, 'I'],
    [/[ĲĴ]/gi, 'J'],
    [/[Ķĸ]/gi, 'J'],
    [/[ŁĽĹĻĿ]/gi, 'L'],
    [/[ÑŃŇŅŉŊ]/gi, 'N'],
    [/[ÒÓÔÕÖØŌŐŎŎ]/gi, 'O'],
    [/Œ/gi, 'OE'],
    [/Ą/gi, 'Q'],
    [/[ŔŘŖ]/gi, 'R'],
    [/[ŚŠŞŜȘ]/gi, 'S'],
    [/[ŤŢŦȚ]/gi, 'T'],
    [/[ÙÚÛÜŪŮŰŬŨŲ]/gi, 'U'],
    [/Ŵ/gi, 'W'],
    [/[ÝŸŶ]/gi, 'Y'],
    [/[ŽŻŹ]/gi, 'Z'],
    [/A/gi, 'A'],
    [/а/gi, 'a'],
    [/Б/gi, 'B'],
    [/б/gi, 'b'],
    [/В/gi, 'V'],
    [/в/gi, 'v'],
    [/Г/gi, 'G'],
    [/г/gi, 'g'],
    [/Д/gi, 'D'],
    [/д/gi, 'd'],
    [/Е/gi, 'E'],
    [/е/gi, 'e'],
    [/Ё/gi, 'Yo'],
    [/ё/gi, 'yo'],
    [/Ж/gi, 'Zh'],
    [/ж/gi, 'zh'],
    [/З/gi, 'Z'],
    [/з/gi, 'z'],
    [/И/gi, 'I'],
    [/и/gi, 'i'],
    [/Й/gi, 'J'],
    [/й/gi, 'j'],
    [/К/gi, 'K'],
    [/к/gi, 'k'],
    [/Л/gi, 'L'],
    [/л/gi, 'l'],
    [/М/gi, 'M'],
    [/м/gi, 'm'],
    [/Н/gi, 'N'],
    [/н/gi, 'n'],
    [/О/gi, 'O'],
    [/о/gi, 'o'],
    [/П/gi, 'P'],
    [/п/gi, 'p'],
    [/Р/gi, 'R'],
    [/р/gi, 'r'],
    [/С/gi, 'S'],
    [/с/gi, 's'],
    [/Т/gi, 'T'],
    [/т/gi, 't'],
    [/У/gi, 'U'],
    [/у/gi, 'u'],
    [/Ф/gi, 'F'],
    [/ф/gi, 'f'],
    [/Х/gi, 'X'],
    [/х/gi, 'x'],
    [/Ц/gi, 'Cz'],
    [/ц/gi, 'cz'],
    [/Ч/gi, 'Ch'],
    [/ч/gi, 'ch'],
    [/Ш/gi, 'Sh'],
    [/ш/gi, 'sh'],
    [/Щ/gi, 'Shh'],
    [/щ/gi, 'shh'],
    [/Ы/gi, 'Y'],
    [/ы/gi, 'y'],
    [/Э/gi, 'E'],
    [/э/gi, 'e'],
    [/Ю/gi, 'Yu'],
    [/ю/gi, 'yu'],
    [/Я/gi, 'Ya'],
    [/я/gi, 'ya']
];
function sanatize(str) {
  var out = subs.reduce(function (str, rpl) {
    return str.replace(rpl[0], rpl[1]);
  }, str).toLowerCase().replace(/&.+?;/g, ' ').replace(/[^a-z0-9_]/g, ' ').replace(/\s+/g, ' ').trim().replace(/\s/g, '_');
  if (out.match(/^[^a-z_]/) || reserved.has(out)) {
    out = '_' + out;
  }
  return out;
}
