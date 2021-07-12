/*
 * Software in the Sustain Ecosystem are Released Under Terms of Apache Software License
 * This research has been supported by funding from the US National Science Foundation’s CSSI program through awards 1931363, 1931324, 1931335, and 1931283.
 * The project is a joint effort involving Colorado State University, Arizona State University, the University of California-Irvine, and the University of Maryland – Baltimore County.
 * All re-distributions of the software must also include this information.
 *
 * TERMS AND CONDITIONS FOR USE, REPRODUCTION, AND DISTRIBUTION
 *
 *     Definitions.
 *
 *     "License" shall mean the terms and conditions for use, reproduction, and distribution as defined by Sections 1 through 9 of this document.
 *
 *     "Licensor" shall mean the copyright owner or entity authorized by the copyright owner that is granting the License.
 *
 *     "Legal Entity" shall mean the union of the acting entity and all other entities that control, are controlled by, or are under common control with that entity. For the purposes of this definition, "control" means (i) the power, direct or indirect, to cause the direction or management of such entity, whether by contract or otherwise, or (ii) ownership of fifty percent (50%) or more of the outstanding shares, or (iii) beneficial ownership of such entity.
 *
 *     "You" (or "Your") shall mean an individual or Legal Entity exercising permissions granted by this License.
 *
 *     "Source" form shall mean the preferred form for making modifications, including but not limited to software source code, documentation source, and configuration files.
 *
 *     "Object" form shall mean any form resulting from mechanical transformation or translation of a Source form, including but not limited to compiled object code, generated documentation, and conversions to other media types.
 *
 *     "Work"shall mean the work of authorship, whether in Source or Object form, made available under the License, as indicated by a copyright notice that is included in or attached to the work (an example is provided in the Appendix below).
 *
 *     "Derivative Works"shall mean any work, whether in Source or Object form, that is based on (or derived from) the Work and for which the editorial revisions, annotations, elaborations, or other modifications represent, as a whole, an original work of authorship. For the purposes of this License, Derivative Works shall not include works that remain separable from, or merely link (or bind by name) to the interfaces of, the Work and Derivative Works thereof.
 *
 *     "Contribution"shall mean any work of authorship, including the original version of the Work and any modifications or additions to that Work or Derivative Works thereof, that is intentionally submitted to Licensor for inclusion in the Work by the copyright owner or by an individual or Legal Entity authorized to submit on behalf of the copyright owner. For the purposes of this definition, "submitted" means any form of electronic, verbal, or written communication sent to the Licensor or its representatives, including but not limited to communication on electronic mailing lists, source code control systems, and issue tracking systems that are managed by, or on behalf of, the Licensor for the purpose of discussing and improving the Work, but excluding communication that is conspicuously marked or otherwise designated in writing by the copyright owner as "Not a Contribution."
 *
 *     "Contributor"shall mean Licensor and any individual or Legal Entity on behalf of whom a Contribution has been received by Licensor and subsequently incorporated within the Work.
 *
 *     Grant of Copyright License.
 *
 *     Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable copyright license to reproduce, prepare Derivative Works of, publicly display, publicly perform, sublicense, and distribute the Work and such Derivative Works in Source or Object form.
 *     Grant of Patent License.
 *
 *     Subject to the terms and conditions of this License, each Contributor hereby grants to You a perpetual, worldwide, non-exclusive, no-charge, royalty-free, irrevocable (except as stated in this section) patent license to make, have made, use, offer to sell, sell, import, and otherwise transfer the Work, where such license applies only to those patent claims licensable by such Contributor that are necessarily infringed by their Contribution(s) alone or by combination of their Contribution(s) with the Work to which such Contribution(s) was submitted. If You institute patent litigation against any entity (including a cross-claim or counterclaim in a lawsuit) alleging that the Work or a Contribution incorporated within the Work constitutes direct or contributory patent infringement, then any patent licenses granted to You under this License for that Work shall terminate as of the date such litigation is filed.
 *     Redistribution.
 *
 *     You may reproduce and distribute copies of the Work or Derivative Works thereof in any medium, with or without modifications, and in Source or Object form, provided that You meet the following conditions:
 *         You must give any other recipients of the Work or Derivative Works a copy of this License; and
 *         You must cause any modified files to carry prominent notices stating that You changed the files; and
 *         You must retain, in the Source form of any Derivative Works that You distribute, all copyright, patent, trademark, and attribution notices from the Source form of the Work, excluding those notices that do not pertain to any part of the Derivative Works; and
 *         If the Work includes a "NOTICE" text file as part of its distribution, then any Derivative Works that You distribute must include a readable copy of the attribution notices contained within such NOTICE file, excluding those notices that do not pertain to any part of the Derivative Works, in at least one of the following places: within a NOTICE text file distributed as part of the Derivative Works; within the Source form or documentation, if provided along with the Derivative Works; or, within a display generated by the Derivative Works, if and wherever such third-party notices normally appear. The contents of the NOTICE file are for informational purposes only and do not modify the License. You may add Your own attribution notices within Derivative Works that You distribute, alongside or as an addendum to the NOTICE text from the Work, provided that such additional attribution notices cannot be construed as modifying the License.
 *
 *     You may add Your own copyright statement to Your modifications and may provide additional or different license terms and conditions for use, reproduction, or distribution of Your modifications, or for any such Derivative Works as a whole, provided Your use, reproduction, and distribution of the Work otherwise complies with the conditions stated in this License.
 *     Submission of Contributions.
 *
 *     Unless You explicitly state otherwise, any Contribution intentionally submitted for inclusion in the Work by You to the Licensor shall be under the terms and conditions of this License, without any additional terms or conditions. Notwithstanding the above, nothing herein shall supersede or modify the terms of any separate license agreement you may have executed with Licensor regarding such Contributions.
 *     Trademarks.
 *
 *     This License does not grant permission to use the trade names, trademarks, service marks, or product names of the Licensor, except as required for reasonable and customary use in describing the origin of the Work and reproducing the content of the NOTICE file.
 *     Disclaimer of Warranty
 *
 *     Unless required by applicable law or agreed to in writing, Licensor provides the Work (and each Contributor provides its Contributions) on an “AS IS” BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied, including, without limitation, any warranties or conditions of TITLE, NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE. You are solely responsible for determining the appropriateness of using or redistributing the Work and assume any risks associated with Your exercise of permissions under this License.
 *     Limitation of Liability
 *
 *     In no event and under no legal theory, whether in tort (including negligence), contract, or otherwise, unless required by applicable law (such as deliberate and grossly negligent acts) or agreed to in writing, shall any Contributor be liable to You for damages, including any direct, indirect, special, incidental, or consequential damages of any character arising as a result of this License or out of the use or inability to use the Work (including but not limited to damages for loss of goodwill, work stoppage, computer failure or malfunction, or any and all other commercial damages or losses), even if such Contributor has been advised of the possibility of such damages.
 *
 * END OF TERMS AND CONDITIONS
 */

import fs = require('fs');
import { randomString } from './util'
import csv = require('csv-parser');
import readline = require('readline');
import { importCSV, importJSON, appendJSONList, createIndexes } from './mongoBindings';
import path = require('path')

type dictionary = {
    [key: string]: string
}

type dictionaryAny = {
    [key: string]: any
}

//welp, atleast theres not really any external dependencies.
const ingestNeon = async (name: string, filepath: string, tableName: string) => {
    let files: dictionary[] = fs.readdirSync(filepath).map(fileName => {
        return {
            site: fileName.split('_')[0],
            month: fileName.split('_')[1],
            fileName
        }
    });
    files = files.sort((a, b) => new Date(a.month).valueOf() - new Date(b.month).valueOf());
    const productCode = path.basename(filepath)

    const findFileMatch = (regexp: RegExp, fileList = files.map(file => file.fileName)) => {
        for (const fileName of fileList) {
            if (fileName.match(regexp)) {
                return fileName;
            }
        }
    }

    const uniqueRun = randomString(4);
    console.log(`Unique code for this run is ${uniqueRun}`)

    const timeSeriesFileName = `${name}_time_series_${uniqueRun}.csv`
    console.log(`Creating Time Series: ${timeSeriesFileName}`)
    fs.appendFileSync(`./out/${timeSeriesFileName}`, 'site,epoch_time,');

    const labelMapFileName = `${name}_label_map_${uniqueRun}.json`
    console.log(`Creating Label map: ${labelMapFileName}`)
    fs.openSync(`./out/${labelMapFileName}`, 'w');

    const locationsFileName = `${name}_locations_${uniqueRun}.json`
    console.log(`Creating Locations: ${locationsFileName}`)
    fs.openSync(`./out/${locationsFileName}`, 'w');
    let locationsGeojsonList: dictionaryAny[] = [];

    let timeSeriesHeaderIsDone = false;
    let labelMapIsDone = false;
    for (const site of [...new Set(files.map(file => file.site))]) {
        const siteID = `${site}_${productCode}`;
        console.log(siteID);

        let locationsForSiteCode: string[] = [];
        let geojsonPositionsForSiteCode: dictionaryAny[] = [];
        let locsWhichAreUsed: Set<string> = new Set();
        for (const { month, fileName } of files.filter(file => file.site === site)) {
            //console.log(`Package folder is ${fileName}`)
            const innerFileList = fs.readdirSync(`${filepath}/${fileName}`)
            if (!locationsForSiteCode.length) {
                console.log(`Getting sensor pos for ${siteID}`)
                for (const file of innerFileList) {
                    if (file.match(/^.*sensor_positions.*.csv$/g)) {
                        console.log(`File with positions is: ${file}`)
                        const positions = await new Promise<dictionaryAny[]>(resolve => {
                            let datum: dictionaryAny[] = [];
                            const posStream = fs.createReadStream(`${filepath}/${fileName}/${file}`)
                                .pipe(csv())
                                .on('data', (data) => {
                                    data.loc = data['HOR.VER'];
                                    datum.push(data);
                                })
                                .on('end', () => { resolve(datum) })
                        });

                        const geojsonPositions = positions.map(position => {
                            const site = `${siteID}_${position?.loc}`
                            return {
                                "type": "Feature",
                                "geometry": {
                                    "type": "Point",
                                    "coordinates": [Number(position?.referenceLongitude), Number(position?.referenceLatitude)]
                                },
                                "properties": {
                                    ...position,
                                    site,
                                    collectionName: name,
                                    name: position?.name
                                },
                                site
                            }
                        });

                        geojsonPositionsForSiteCode.push(...geojsonPositions);
                        locationsForSiteCode.push(...positions.map(position => position.loc))
                        break;
                    }
                }

            }

            if (!labelMapIsDone) {
                console.log(`Buidling label map for ${name}`)
                let foundLabels = false;
                for (const file of innerFileList) {
                    if (file.match(/^.*variables.*\.csv$/g)) {
                        console.log(`File with mapping is: ${file}`)
                        let mapping: {}[] = [];
                        mapping.push({
                            "name": "epoch_time",
                            "label": "Date",
                            "type": "date",
                            "step": "day"
                        });
                        await new Promise<void>(resolve => {
                            fs.createReadStream(`${filepath}/${fileName}/${file}`)
                                .pipe(csv())
                                .on('data', ({ table, fieldName, description, units }) => {
                                    if (table === tableName) {
                                        foundLabels = true;
                                        mapping.push({
                                            name: fieldName,
                                            label: description,
                                            unit: units,
                                            hideByDefault: true
                                        })
                                    }
                                })
                                .on('end', () => { resolve() })
                        });
                        fs.writeFileSync(`./out/${labelMapFileName}`, JSON.stringify({
                            collection: name,
                            icon: "science",
                            temporal: "epoch_time",
                            source: `https://data.neonscience.org/data-products/${productCode}`,
                            linked: {
                                collection: "neon_sites",
                                field: "site"
                            },
                            fieldMetadata: mapping
                        }, null, 4));
                        break;
                    }
                }
                labelMapIsDone = foundLabels;
            }

            const tsRegex = new RegExp(`^.*${tableName}.*$`)
            let tsFiles: string[] = [];
            for (const file of innerFileList) {
                if (file.match(tsRegex)) {
                    tsFiles.push(file);
                    console.log(`Time series file is ${file}`)
                }
            }


            if (!timeSeriesHeaderIsDone && tsFiles.length) {
                console.log(`Adding time series header to out!`)
                const fileStream = fs.createReadStream(`${filepath}/${fileName}/${tsFiles[0]}`);
                const rl = readline.createInterface({
                    input: fileStream,
                    crlfDelay: Infinity
                });
                for await (const line of rl) {
                    fs.appendFileSync(`./out/${timeSeriesFileName}`, `${line}\n`);
                    break;
                }
                timeSeriesHeaderIsDone = true;
            }

            for (const tsFile of tsFiles) {
                const locationAtSite = locationsForSiteCode.find(loc => { 
                    const matchStr = `^.*${loc.replace(".","\.")}\..*\.${tableName}.*$`
                    return tsFile.match(new RegExp(matchStr)) 
                });

                if(!locationAtSite){
                    throw "bad"
                }
                locsWhichAreUsed.add(locationAtSite);

                const site = `${siteID}_${locationAtSite}`
                console.log(`Collection epoch times from ${tsFile}`)
                let times: number[] = [];
                await new Promise<void>(resolve => {
                    fs.createReadStream(`${filepath}/${fileName}/${tsFile}`)
                        .pipe(csv())
                        .on('data', (data) => { times.push(new Date(data.startDateTime).valueOf()) })
                        .on('end', () => { resolve() })
                });
                console.log(`Pasting time series for ${tsFile}`)
                const fileStream = fs.createReadStream(`${filepath}/${fileName}/${tsFile}`);
                const rl = readline.createInterface({
                    input: fileStream,
                    crlfDelay: Infinity
                });
                let index = 0;
                for await (const line of rl) {
                    if (index !== 0) {
                        const time = times.shift();
                        !line.split(',').includes('') && fs.appendFileSync(`./out/${timeSeriesFileName}`, `${site},${time},${line}\n`);
                    }
                    index++;
                }
            }
        }
        locationsGeojsonList.push(...geojsonPositionsForSiteCode.filter(location => locsWhichAreUsed.has(location.properties.loc)));
        fs.writeFileSync(`./out/${locationsFileName}`, JSON.stringify(locationsGeojsonList, null, 4));
    }

    console.log(`Importing ${name} locations.`)
    fs.writeFileSync(`./out/${locationsFileName}`, JSON.stringify(locationsGeojsonList, null, 4));
    await appendJSONList('neon_sites', `./out/${locationsFileName}`);

    console.log(`Importing ${name} time series.`)
    await importCSV(name, `./out/${timeSeriesFileName}`);

    console.log(`Creating indexes for ${name}`)
    await createIndexes(name, ['site', 'epoch_time']);

    console.log("done")
}

export default ingestNeon;