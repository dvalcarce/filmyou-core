/**
 * Copyright 2013 Daniel Valcarce Silva
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.udc.fi.dc.irlab.testdata;

/**
 * Contains data for Clustering Assignment purposes.
 * 
 */
public class ClusteringTestData {

    public static final int numberOfUsers = 30;
    public static final int numberOfClusters = 5;

    public static final double[][] H = new double[][] {
	    { 0.01961829174741276, 0.5565497520554153, 0.04846027622728889,
		    0.2894466633626602, 0.0859250166072229 },
	    { 6.244857340271865e-06, 0.02111104713440997, 0.2933183406605639,
		    0.5222572184984003, 0.1633071488492854 },
	    { 0.4143752939867912, 0.007204926800987801, 0.1301776840784583,
		    0.2008351779846832, 0.2474069171490796 },
	    { 0.1223787928820277, 0.3936570901311572, 1.386828387270314e-05,
		    0.4839502455531534, 3.149788950334987e-09 },
	    { 0.2273629321874389, 0.008256749472931965, 0.3392143461136063,
		    0.2596739625250583, 0.1654920097009646 },
	    { 0.229538991946262, 0.04011539522422972, 0.3959750854549406,
		    0.3343705273745674, 2.533806618300665e-16 },
	    { 1.099835697340415e-08, 0.2089304346313727, 0.4531860477961331,
		    0.2532562707746198, 0.08462723579951735 },
	    { 0.01982009233619167, 0.07635808553359551, 0.5190175112361698,
		    0.2378756868489509, 0.1469286240450922 },
	    { 0.1377228253225221, 0.5643198190261187, 0.2494050959576655,
		    0.04855203514440237, 2.245492911648088e-07 },
	    { 0.1597290006900398, 0.04940483868325361, 0.3537808435839855,
		    0.4326718282728204, 0.004413488769900682 },
	    { 0.6641639021714584, 0.2082532182966456, 0.07469077613276542,
		    0.05289209815008147, 5.249048989290964e-09 },
	    { 0.1537031531260713, 0.5298136272985067, 0.004330541084227668,
		    0.3052404366018704, 0.006912241889323952 },
	    { 0.5053842660631992, 0.0001683935938151314, 3.284497884242829e-10,
		    0.07332158055593313, 0.4211257594586028 },
	    { 5.234233194358006e-10, 0.3967410228163543, 0.04241267100360056,
		    0.4515494553718744, 0.1092968502847475 },
	    { 4.463321470171224e-07, 0.5162425155680235, 1.32064369219843e-12,
		    0.01818868537389075, 0.4655683527246181 },
	    { 0.2704293351846697, 0.01383502410540757, 0.02730991022611751,
		    0.3253986022845288, 0.3630271281992765 },
	    { 0.4147590173168751, 0.2589032618900721, 5.746524308733547e-06,
		    4.588719156946562e-07, 0.3263315153968285 },
	    { 1.064943063411391e-06, 3.901057345214725e-06, 0.1286632367607318,
		    0.5522779384211489, 0.3190538588177108 },
	    { 4.604443449816756e-08, 6.914586704154356e-06, 0.6470364592173817,
		    0.05435044594788233, 0.2986061342035973 },
	    { 0.5175178809416368, 0.0687320188703866, 0.0002148989335557507,
		    0.1118582650079035, 0.3016769362465176 },
	    { 0.03315723423730861, 0.2213321965340515, 0.5282155156764666,
		    5.844777781969156e-08, 0.2172949951043956 },
	    { 0.2786152809735006, 0.1932634940559658, 0.1073437154214488,
		    0.4189518205631304, 0.001825688985954352 },
	    { 0.3756312885793482, 1.20029545661482e-12, 1.861739998814503e-09,
		    0.3613540087778115, 0.2630147007799 },
	    { 0.08537365844326679, 0.2744632616445638, 0.1834285886845031,
		    1.0246173719439e-10, 0.4567344911252045 },
	    { 0.1350724732985143, 1.51139327309945e-05, 0.5396253015308372,
		    3.338002622702443e-07, 0.3252867774376554 },
	    { 0.09880685862227936, 0.3810167579205681, 0.002156146945445261,
		    0.2400130904226409, 0.2780071460890662 },
	    { 0.1084254654961067, 0.4612831287644167, 0.430291405739462,
		    3.073079927707493e-15, 1.152095735765425e-14 },
	    { 7.0258220961238e-05, 0.3651321212119053, 0.3151155869648762,
		    3.966666573409727e-10, 0.3196820332055906 },
	    { 0.0003911599769677397, 0.04471107718714283, 0.3090966491725055,
		    0.4735711594441678, 0.1722299542192162 },
	    { 0.5900940280931691, 1.531576462964527e-12, 0.3742406721491912,
		    4.535761621239282e-06, 0.03566076399448684 } };

    public static final int[] clustering = new int[] { 2, 4, 1, 4, 3, 3, 3, 3,
	    2, 4, 1, 2, 1, 4, 2, 5, 1, 4, 3, 1, 3, 4, 1, 5, 3, 2, 2, 2, 4, 1 };

    public static final int[] clusteringCount = new int[] { 7, 7, 7, 7, 2 };

}